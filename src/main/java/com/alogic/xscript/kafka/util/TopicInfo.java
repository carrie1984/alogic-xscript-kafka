package com.alogic.xscript.kafka.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import kafka.admin.AdminUtils;
import kafka.api.LeaderAndIsr;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.collection.Map;
import scala.collection.Seq;

public class TopicInfo {
	 protected  int partitionCount;
	 protected int ReplicationFactor;
	 protected java.util.Properties config;
	 
	 protected List<HashMap<String, Object>> partitionDetail;
	 public TopicInfo()
	 {
		 partitionCount = 0;
		 ReplicationFactor = 0;
		 config = new java.util.Properties();
		 partitionDetail = new ArrayList<>();
	 }
	 public HashMap<String, Object> getTopicInfo(KKConnector conn,String topicName)
	 {
		HashMap<String, Object> AllInfo = new HashMap<>();
		ZkUtils zkUtils = KKConnector.zkUtils;
		
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

}
