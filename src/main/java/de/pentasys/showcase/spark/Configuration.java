package de.pentasys.showcase.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import de.pentasys.showcase.spark.kafka.StreamConsumer;
import de.pentasys.showcase.spark.service.ProcessExecutor;

public class Configuration {

	// ############################
	// #### KAFKA CONFIGURATION ###
	// ############################
	public static final String KAFKA_ADDRESS = "broker.kafka.l4lb.thisdcos.directory:9092";
	public static final Collection<String> KAFKA_SAMPLE_TOPICS = Arrays.asList("streamTopic");
	private static Map<String, Object> kafkaConsumerParameter = new HashMap<>();
	private static Map<String, Object> kafkaProducerParameter = new HashMap<>();

	public Configuration() {
		kafkaConsumerParameter.put("bootstrap.servers", KAFKA_ADDRESS);
		kafkaConsumerParameter.put("key.deserializer", StringDeserializer.class);
		kafkaConsumerParameter.put("value.deserializer", StringDeserializer.class);
		kafkaConsumerParameter.put("group.id", "id-" + Math.random());
		kafkaConsumerParameter.put("auto.offset.reset", "earliest");
		kafkaConsumerParameter.put("enable.auto.commit", false);

		kafkaProducerParameter.put("bootstrap.servers", KAFKA_ADDRESS);
		kafkaProducerParameter.put("acks", "all");
		kafkaProducerParameter.put("retries", 0);
		kafkaProducerParameter.put("batch.size", 16384);
		kafkaProducerParameter.put("linger.ms", 1);
		kafkaProducerParameter.put("buffer.memory", 33554432);
		kafkaProducerParameter.put("key.deserializer", StringDeserializer.class);
		kafkaProducerParameter.put("value.deserializer", StringDeserializer.class);
	}

	protected JavaSparkContext createSparkContext() {
		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(new Class<?>[] { 
			ConsumerRecord.class, 
			String.class,
			ConsumerRecord[].class, 
			TimestampType.class, 
			MemoryRecords.class, 
			LogEntry.class, 
			Record.class,
			InvalidRecordException.class,
			StreamConsumer.class,
			JavaStreamingContext.class,
			ProcessExecutor.class});
		conf.set("spark.kryo.registrationRequired", "true");
		conf.set("spark.cores.max","1");
		conf.setAppName("simpleHelloWorldJob");
		
		return new JavaSparkContext(conf);
	}

	public static Map<String, Object> getKafkaConsumerParameter() {
		return kafkaConsumerParameter;
	}

	public static Map<String, Object> getKafkaProducerParameter() {
		return kafkaProducerParameter;
	}
	
}