package com.alogic.xscript.kafka.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * kafka对topic进行管理时需要的连接类
 * @author cuijialing
 */
public class KKConnector {
	/*
	 * a logger of slf4j
	 */
	protected static final Logger logger = LoggerFactory.getLogger(KKConnector.class);
	/*
	 * kafka的连接参数
	 */
	protected String zookeeperConnect = "$zookeeperConnect";
	protected int sessionTimeoutMs;
	protected int connectionTimeoutMs;
	
	public  ZkUtils zkUtils;
	public ZkClient zkClient;
	public boolean isSecureKafkaCluster = false;
	public KKConnector(Properties props)
	{
		zookeeperConnect = PropertiesConstants.getString(props, "zookeeperConnect",zookeeperConnect);
		sessionTimeoutMs = PropertiesConstants.getInt(props, "sessionTimeoutMs", sessionTimeoutMs);
		connectionTimeoutMs = PropertiesConstants.getInt(props, "connectionTimeoutMs", connectionTimeoutMs);
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
		sessionTimeoutMs = sessiontime;
		connectionTimeoutMs = connectiontime;
		zkClient = new ZkClient(
			        zookeeperConnect,
			        sessionTimeoutMs,
			        connectionTimeoutMs,
			        ZKStringSerializer$.MODULE$); 
		connect();
     }
	/*
	 * 连接zookeeper
	 */
	public void connect()
	{
		 zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
	}
	/*
	 * 断开zookeeper
	 */
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
	public void createTopic(String topicName,int partition,int replication,java.util.Properties configs)
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
	
	
	//修改topic，只能增加分区数目
	public void alterTopic(String topicName,String param)
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
	    	TopicCommand.main(optionsAddPartitions); 
	}
	//查看所有topic列表信息
	public  List<String> ListTopic()
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
	
	//查看topic详细信息，此方法暂不使用，实际调用的是util中的TopicInfo类中的方法
	//
	public HashMap<String, Object>  describeTopic(String topicName)
	{
		 int partitionCount = 0;
		 int ReplicationFactor = 0;
		 java.util.Properties config = new java.util.Properties();
		 
		 List<HashMap<String, Object>> partitionDetail = new ArrayList<>();
		 HashMap<String, Object> AllInfo = new HashMap<>();
			
			scala.collection.Seq<String> allTopics = zkUtils.getAllTopics();
			Map<String, Seq<Object>> PartitionsForTopics = zkUtils.getPartitionsForTopics(allTopics);
			Seq<Object> partitionNum = PartitionsForTopics.apply(topicName);
			partitionCount = partitionNum.size();
			
			Map<String, Map<Object, Seq<Object>>> replicaTopics = zkUtils.getPartitionAssignmentForTopics(allTopics);
			Map<Object, Seq<Object>> PartitionAssignment = replicaTopics.apply(topicName);
			//TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, PartitionCount);
			ReplicationFactor = PartitionAssignment.head()._2.size();
			
			Map<String, java.util.Properties> topicsConfig = AdminUtils.fetchAllTopicConfigs(zkUtils);
			config = topicsConfig.apply(topicName);
			
			for(int i=0;i<partitionCount;++i)
			{
				HashMap<String, Object> perPartition = new HashMap<>();			
				perPartition.put("partition", i);
				
				
				Option<LeaderAndIsr> leaderIsr = zkUtils.getLeaderAndIsrForPartition(topicName, i);
				Seq<Object> SyncReplicas = zkUtils.getInSyncReplicasForPartition(topicName, i);
				List<Object> repList = new ArrayList<>();
				for(int k=0;k<SyncReplicas.length();++k)
				{
					repList.add(SyncReplicas.apply(k));
				}
				perPartition.put("SyncReplicas",repList );
				
				
				List<Object> IsrList = new ArrayList<Object>();
				scala.collection.immutable.List<Object> Isr = leaderIsr.get().isr();	    
				int IsrNum = Isr.size();
			    for(int j=0;j<IsrNum;++j)
			    {
			    	IsrList.add(Isr.apply(j));
			    }
			    perPartition.put("IsrList", IsrList);
			    
			    
			    int Leader = leaderIsr.get().leader();
			    perPartition.put("Leader", Leader);

			    
			    partitionDetail.add(perPartition);
			    
			}
			
			HashMap<String, Object> hashsum = new HashMap<>(); 
			hashsum.put("partitionCount", partitionCount);
			hashsum.put("ReplicationFactor", ReplicationFactor);
			hashsum.put("config", config);
			AllInfo.put("sum",hashsum);
			AllInfo.put("detail", partitionDetail);
			
			return AllInfo;
	}
	
	//删除topic
	public void deleteTopic(String topicName)
	{
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
