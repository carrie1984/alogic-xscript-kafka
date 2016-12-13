package com.alogic.xscript.kafka.util;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;


import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.api.LeaderAndIsr;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.collection.Map;
import scala.collection.Seq;
/*
 * 20161209 cuijialing create
 */
public class KKConnector {
	/*
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(KKConnector.class);
	/*
	 * kafka的连接参数
	 */
	protected String zookeeperConnect = "$server";
	protected int sessionTimeoutMs;
	protected int connectionTimeoutMs;
	
	public static ZkUtils zkUtils;
	public ZkClient zkClient;
	public boolean isSecureKafkaCluster = false;
	public KKConnector(Properties props,int sessiontime,int connectiontime)
	{
		zookeeperConnect = PropertiesConstants.getString(props, "zookeeperConnect",zookeeperConnect);
		zkClient = new ZkClient(
			        zookeeperConnect,
			        sessionTimeoutMs,
			        connectionTimeoutMs,
			        ZKStringSerializer$.MODULE$); 
		connect(); 	 
	}
	public KKConnector(Properties props,String servers,int sessiontime,int connectiontime)
	{
		zookeeperConnect = servers;
		zkClient = new ZkClient(
			        zookeeperConnect,
			        sessionTimeoutMs,
			        connectionTimeoutMs,
			        ZKStringSerializer$.MODULE$); 
		connect();
     }
	public void connect()
	{
		 zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
	}
	public void disconnect()
	{
		zkUtils.close();
	}
	//是否是安全的连接
	public boolean isSecure()
	{
		return zkUtils.isSecure();
	}
	//重新连接
	public void reconnect()
	{
		disconnect();
		connect();
	}
	
	//topic有关操作，包括create，alter，list，describe，delete
	//创建topic
	public static void createTopic(String topicName,int partition,int replication,java.util.Properties configs)
	{
		if(AdminUtils.topicExists(zkUtils, topicName))
	    {
			logger.error("This topic has already been created");
	    }
		else
		{
			AdminUtils.createTopic(zkUtils, topicName, partition, replication, configs,AdminUtils.createTopic$default$6());		    
		}
	}
	
	
	//修改topic
	public static void alterTopic(String type,String topicName,String param)
	{
		if(type!="partitions"&&type!="config"&&type!="deleteconfig")
		{
			logger.error("this type is not support.");
		}
		switch(type)
		{
		case"partitions":
		{
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
		    	optionsAddPartitions[6] = param;
		}
		case"config":
		{
			String[] optionsAddConfigs = new String[]{  
		    	    "--alter",  
		    	    "--zookeeper",  
		    	    "localhost:2181",  
		    	    "--topic",  
		    	    "my_topic_name",  
		    	    "--config",  
		    	    "item"  
		    	};  
	    	optionsAddConfigs[4] = topicName;
	    	optionsAddConfigs[6] = param;
	    	TopicCommand.main(optionsAddConfigs); 
		}
		case"deleteconfig":
		{
			String[] optionsDeleteConfigs = new String[]{  
		    	    "--alter",  
		    	    "--zookeeper",  
		    	    "localhost:2181",  
		    	    "--topic",  
		    	    "my_topic_name",  
		    	    "--deleteconfig",  
		    	    "item"  
		    	};  
	    	optionsDeleteConfigs[4] = topicName;
	    	optionsDeleteConfigs[6] = param;
	    	TopicCommand.main(optionsDeleteConfigs); 
		}
		}
	}
	//查看所有topic列表信息
	public static List<String> ListTopic()
	{
		scala.collection.Seq<String> allTopics = zkUtils.getAllTopics();
		scala.collection.immutable.List<String> list = allTopics.toList();
		List<String> topiclist = new ArrayList<>();
		int size = list.size();
		for(int i=0;i<size;++i)
		{
			topiclist.add(i, list.apply(i));
		}
		return topiclist;
	}
	
	//查看topic详细信息
	//初始设定需要输出的参数值,参数需要进行初始化，否则会出现空指针异常报错
	public static int partitionCount = 0;
	public static int ReplicationFactor = 0;
	public static java.util.Properties config = new java.util.Properties();
	public static List<Integer> partitionList = new ArrayList<>();
	public static List<Seq<Object>> replicasList = new ArrayList<>();
	public static List<Integer> leaderList = new ArrayList<>();
	public static List<List<Object>> AllIsrList = new ArrayList<>();

	
	public static void  describeTopic(String topicName)
	{
		scala.collection.Seq<String> allTopics = zkUtils.getAllTopics();
		scala.collection.immutable.List<String> list = allTopics.toList();

		Map<String, Seq<Object>> PartitionsForTopics = zkUtils.getPartitionsForTopics(allTopics);
		Seq<Object> partitionNum = PartitionsForTopics.apply(topicName);
		partitionCount = partitionNum.size();//第一行总和第一项=====================
		
		
		Map<String, Map<Object, Seq<Object>>> replicaTopics = zkUtils.getPartitionAssignmentForTopics(allTopics);
		Map<Object, Seq<Object>> PartitionAssignment = replicaTopics.apply(topicName);
		//TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, PartitionCount);
		ReplicationFactor = PartitionAssignment.size();
		//System.out.println(info);				
//		System.out.println("ReplicationFactor===="+ReplicationFactor);
		
		Map<String, java.util.Properties> topicsConfig = AdminUtils.fetchAllTopicConfigs(zkUtils);
		config = topicsConfig.apply(topicName);
		//Properties topicConfig = AdminUtils.fetchEntityConfig(zkUtils, topicName, topicName);
//		boolean flag = config.isEmpty();
//		System.out.println(config.size());
//		System.out.println("configs=========="+config.toString());
		
		
		
		List<Object> IsrList = new ArrayList<Object>();
//		System.out.println("partitionNum "+partitionCount);
		for(int i=0;i<partitionCount;++i)
		{
			Object temp = partitionNum.apply(0).toString();	
			int num = Integer.parseInt(temp.toString());
			partitionList.add(i, num);
//			System.out.println("partition "+num+" ");//第二行第一项
			Option<LeaderAndIsr> leaderIsr = zkUtils.getLeaderAndIsrForPartition(topicName, num);
			Seq<Object> SyncReplicas = zkUtils.getInSyncReplicasForPartition(topicName, num);
			replicasList.add(i, SyncReplicas);
//			System.out.println("======Replics===========");
//			for(int k=0;k<SyncReplicas.length();++k)
//			{
//				System.out.print(SyncReplicas.apply(k)+" ");
//				System.out.println("");
//			}
			//System.out.println(SyncReplicas.length());
			scala.collection.immutable.List<Object> Isr = leaderIsr.get().isr();
		    int IsrNum = Isr.size();
//		    System.out.println("======Isr=============");
		    for(int j=0;j<IsrNum;++j)
		    {
		    	IsrList.add(Isr.apply(j));
//		    	System.out.println(IsrList.get(j));
		    }
		    
		    AllIsrList.add(i, IsrList);
		    int Leader = leaderIsr.get().leader();
		    leaderList.add(i, Leader);
//		    System.out.println("======Leader=============");
//		    System.out.println(Leader);
		}
	}
	
	//删除topic
	public static void deleteTopic(String topicName)
	{
		 if(!AdminUtils.topicExists(zkUtils, topicName))
		    {
		    	//System.out.println(topicName+" does not exist!");
		    }
		    else
		    {
		    	AdminUtils.deleteTopic(zkUtils, topicName);
		    	//System.out.println(topicName+" is deleted successfully!");
		    }
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
