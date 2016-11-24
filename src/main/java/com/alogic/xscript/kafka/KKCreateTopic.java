package com.alogic.xscript.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import kafka.utils.ZKStringSerializer$;

//bin/kafka-topics.sh --zookeeper zk_host:port/chroot --create --topic my_topic_name --partitions 20 --replication-factor 3 --config x=y
public class KKCreateTopic {
	public static void main(String[] args) {
	    String zookeeperConnect = "localhost:2181";
	    int sessionTimeoutMs = 10 * 1000;
	    int connectionTimeoutMs = 8 * 1000;
	    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
	    // createTopic() will only seem to work (it will return without error).  The topic will exist in
	    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
	    // topic.
	    ZkClient zkClient = new ZkClient(
	        zookeeperConnect,
	        sessionTimeoutMs,
	        connectionTimeoutMs,
	        ZKStringSerializer$.MODULE$);

	    // Security for Kafka was added in Kafka 0.9.0.0
	    boolean isSecureKafkaCluster = false;
	    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

	    String topic = "my-topic";
	    int partitions = 1;
	    int replication = 1;
	    Properties topicConfig = new Properties(); // add per-topic configurations settings here
	    AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig);
	    zkClient.close();
	  }

}
