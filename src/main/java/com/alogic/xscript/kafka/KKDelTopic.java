package com.alogic.xscript.kafka;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/*
 * String[] options = new String[]{  
    "--zookeeper",  
    "zk_host:port/chroot",  
    "--topic",  
    "my_topic_name"  
};  
DeleteTopicCommand.main(options); 
 */
public class KKDelTopic {
	
	public static void main(String[] args)
	{
		String topicName = "my-topic";
		 String zookeeperConnect = "localhost:2181";
		    int sessionTimeoutMs = 10 * 1000;
		    int connectionTimeoutMs = 8 * 1000;
		ZkClient zkClient = new ZkClient(
		        zookeeperConnect,
		        sessionTimeoutMs,
		        connectionTimeoutMs,
		        ZKStringSerializer$.MODULE$);
		boolean isSecureKafkaCluster = false;
	    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

	    if(!AdminUtils.topicExists(zkUtils, topicName))
	    {
	    	System.out.println(topicName+" does not exist!");
	    }
	    else
	    {
	    	AdminUtils.deleteTopic(zkUtils, topicName);
	    	System.out.println(topicName+" is deleted successfully!");
	    }
	}
}
