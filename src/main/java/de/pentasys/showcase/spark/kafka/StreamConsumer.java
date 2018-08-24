package de.pentasys.showcase.spark.kafka;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import de.pentasys.showcase.spark.Configuration;
import de.pentasys.showcase.spark.service.ProcessExecutor;

public class StreamConsumer implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(StreamConsumer.class);

	private JavaStreamingContext jsc;

	private TopicManagement topicMngm;

	private Integer logCounter;

	public StreamConsumer(JavaSparkContext sc) {
		super();
		jsc = new JavaStreamingContext(sc, new Duration(5000));
		this.topicMngm = new TopicManagement();
		this.logCounter = 0;
	}

	public void consumeKafkaStream() {
		LOG.info("#### consumeKafkaStream() is executed!");

		for (String topic : Iterables.concat(Configuration.KAFKA_SAMPLE_TOPICS)) {
			topicMngm.createTopicIfNotExists(topic);
		}

		try {
			final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
					LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(
							Configuration.KAFKA_SAMPLE_TOPICS, Configuration.getKafkaConsumerParameter()));

			stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
					OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

					ProcessExecutor executor = new ProcessExecutor();
					rdd.foreach(entry -> {
							executor.processTraceLogLines(entry.key(), entry.value());
					});

					((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
					logCounter = logCounter + (int) rdd.count();
					LOG.info("Processed incomming RDDs: {}", logCounter);
				}
			});

			jsc.start();
			jsc.awaitTermination();
		} catch (Exception ex) {
			LOG.error("Exception occured!!!!! ::: {} :::: {}", ex.getMessage(), Arrays.toString(ex.getStackTrace()));
		} finally {
			if (jsc != null) {
				LOG.info("### finally restart again... ");
				consumeKafkaStream();
			}
		}
	}
}
