package com.alogic.xscript.kafka;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;



//bin/kafka-topics.sh --list --zookeeper localhost:2181
public class KKListTopic {
	
//	public KKListTopic(String tag,Logiclet p)
//	{
//		super(tag,p);
//	}
	public static void main(String arg[])
	{
		
//		String[] options = new String[]{  
//			    "--list",  
//			    "--zookeeper",  
//			    "localhost:2181"  
//			};  
//			TopicCommand.main(options);
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
	    
	    scala.collection.Map<String, Properties> topiclist = AdminUtils.fetchAllTopicConfigs(zkUtils);
	    int size = topiclist.keys().size();
	    System.out.println("the num of the topic is "+size);

	    
	    
	    String temp = topiclist.keys().toString();
	    System.out.println(temp);
	    
	    int length = temp.length();
	    String result = temp.substring(4, length-1);
	    
	    result = result.replace(" ", "");
	    String[] strs = result.split(",");
	    System.out.println(strs.length);
	    System.out.println("==========================="); 
//	    
	    for(int i=0;i<strs.length;++i)
	    {
	    	System.out.println(strs[i]);
	    }	  
	    
	}
  
}


