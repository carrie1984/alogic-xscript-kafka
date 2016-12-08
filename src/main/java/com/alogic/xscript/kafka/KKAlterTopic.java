package com.alogic.xscript.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import com.alogic.xscript.Main;

import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

//bin/kafka-topics.sh --zookeeper zk_host:port/chroot --alter --topic my_topic_name --deleteConfig x
public class KKAlterTopic {
	public static void main(){
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

	    String topicName = "my-topic";
	    if(!AdminUtils.topicExists(zkUtils, topicName))
	    {
	    	System.out.println(topicName+" has not been created!");
	    }
	    else 
	    {
		    int numPartitions = 2;
		    //有三种修改的方式，增加分区，增加配置参数，删除配置参数
		    
		    //增加分区
		    String[] optionsAddPartitions = new String[]{  
		    	    "--alter",  
		    	    "--zookeeper",  
		    	    "localhost:2181",  
		    	    "--topic",  
		    	    "my_topic_name",  
		    	    "--partitions",  
		    	    "num"  
		    	};  
		    	optionsAddPartitions[4] = topicName;
		    	optionsAddPartitions[6] = Integer.toString(numPartitions);
		    	TopicCommand.main(optionsAddPartitions); 
		    	String[] optionsAddConfigs = new String[]{  
			    	    "--alter",  
			    	    "--zookeeper",  
			    	    "localhost:2181",  
			    	    "--topic",  
			    	    "my_topic_name",  
			    	    "--config",  
			    	    "item"  
			    	};  
		    	optionsAddPartitions[4] = topicName;
		    	optionsAddPartitions[6] = "flush.messages=1";
		    	TopicCommand.main(optionsAddConfigs); 
		    	String[] optionsDeleteConfigs = new String[]{  
			    	    "--alter",  
			    	    "--zookeeper",  
			    	    "localhost:2181",  
			    	    "--topic",  
			    	    "my_topic_name",  
			    	    "--deleteconfig",  
			    	    "item"  
			    	};  
		    	optionsAddPartitions[4] = topicName;
		    	optionsAddPartitions[6] = "flush.messages=1";
		    	TopicCommand.main(optionsDeleteConfigs); 
		    	
		    
		}

//	    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(arg0, arg1, arg2, arg3, arg4);
	    zkClient.close(); 
	}

}
