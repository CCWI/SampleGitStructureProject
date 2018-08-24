package de.pentasys.showcase.spark.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode.Enforced$;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * Topic Management of Kafka.
 * 
 * @author Max.Auch
 *
 */
public class TopicManagement {

	private static final Logger LOG = LoggerFactory.getLogger(TopicManagement.class);

	private ZkClient zkClient;
	private ZkUtils zkUtils;

	/**
	 * Default-Constructor
	 */
	public TopicManagement() {
		zkClient = new ZkClient("master.mesos:2181/dcos-service-kafka", 10000, 10000, ZKStringSerializer$.MODULE$);
		ZkConnection zkConnection = new ZkConnection("master.mesos:2181/dcos-service-kafka", 10000);
		zkUtils = new ZkUtils(zkClient, zkConnection, false);
	}

	/**
	 * Create new topic, if it does not exists. Returns true if topic is created
	 * successfully or already exists.
	 * 
	 * @param topicName
	 * @return
	 */
	public Boolean createTopicIfNotExists(String topicName) {
		LOG.debug("createTopic is executed with topicName: {}", topicName);
		Boolean success = false;

		if (topicName != null && !AdminUtils.topicExists(zkUtils, topicName)) {
			AdminUtils.createTopic(zkUtils, topicName, 1, 1, new Properties(), Enforced$.MODULE$);
		}

		if (AdminUtils.topicExists(zkUtils, topicName)) {
			success = true;
		}

		return success;
	}

	public void closeClient() {
		if (zkClient != null) {
			zkClient.close();
		}
	}
}
