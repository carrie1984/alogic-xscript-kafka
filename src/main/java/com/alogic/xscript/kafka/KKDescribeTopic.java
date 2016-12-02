package com.alogic.xscript.kafka;

import java.util.Properties;

//未完成  cuijialing

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.requests.MetadataResponse;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.api.TopicMetadata;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.Map;

//bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic

public class KKDescribeTopic {
	
	public static String zookeeperConnect = "localhost:2181";
    public static int sessionTimeoutMs = 10 * 1000;
    public static int connectionTimeoutMs = 8 * 1000;
    public static ZkClient zkClient = new ZkClient(
        zookeeperConnect,
        sessionTimeoutMs,
        connectionTimeoutMs,
        ZKStringSerializer$.MODULE$);
    public static boolean isSecureKafkaCluster = false;
    public String topicName = "test";
    public static ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
	
	public static void describeTopic(ZkUtils zkUtils, TopicCommandOptions opts)
	{
		   Object allTopics = zkUtils.getAllTopics();
		   
	}
	
	public static void main(String[] args){
		
		String topicName = "my-topic";
		String[] options = new String[]{  
			    "--describe",  
			    "--zookeeper",  
			    "localhost:2181",  
			    "--topic",  
			    "default",  
			};  
		options[4] = topicName;
			TopicCommand.main(options);  
			
			 

		    
		    
		    

	}

}
