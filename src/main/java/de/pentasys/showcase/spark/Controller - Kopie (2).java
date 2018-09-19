package de.pentasys.showcase.spark;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.pentasys.showcase.spark.kafka.StreamConsumer;

@SuppressWarnings("serial")
public class Controller implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(Controller.class);

	public static void main(String[] args) {
		LOG.info("#### Controller started!");

		new StreamConsumer(new Configuration().createSparkContext()).consumeKafkaStream();

		LOG.info("#### Spark application stopped!");
	}
}
