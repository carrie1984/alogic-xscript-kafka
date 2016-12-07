package com.alogic.xscript.kafka;

import java.awt.PointerInfo;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//未完成  cuijialing

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.requests.MetadataResponse;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.api.LeaderAndIsr;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.common.Topic;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.annotation.implicitNotFound;
import scala.collection.Map;
import scala.collection.Seq;
import scala.collection.parallel.ParIterableLike.Partition;

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
   // public String topicName = "my-topic";
    public static ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

	public static void describeTopic()
	{
		
		   scala.collection.Seq<String> allTopics = zkUtils.getAllTopics();
		   int size = allTopics.size();
		   //System.out.println(size);
		   String topicName = "my-topic";
		scala.collection.immutable.List<String> list = allTopics.toList();
//		Map<String, Map<Object, Seq<Object>>> PartitionAssignmentForTopics  = zkUtils.getPartitionAssignmentForTopics(allTopics);
//		Map<Object, Seq<Object>> PerInfo = PartitionAssignmentForTopics.apply(topicName);
//		
//		System.out.println(PerInfo.toString());
		Map<String, Seq<Object>> PartitionsForTopics = zkUtils.getPartitionsForTopics(allTopics);
		Seq<Object> partitionNum = PartitionsForTopics.apply(topicName);
		int PartitionCount = partitionNum.size();//第一行总和第一项=====================
		
		System.out.println("partitionNum "+PartitionCount);
		for(int i=0;i<PartitionCount;++i)
		{
			Object temp = partitionNum.apply(0).toString();	
			int num = Integer.parseInt(temp.toString());
			System.out.print("partition "+num+" ");//第二行第一项
			Option<LeaderAndIsr> leaderIsr = zkUtils.getLeaderAndIsrForPartition(topicName, num);
			String leader = leaderIsr.toString();
			leader = leader.substring(4);
			leader = leader.replace("(", "");
			leader = leader.replace(")", "");
			leader = leader.replace("{", "");
			leader = leader.replace("}", "");
			leader = leader.replace("\"", "");
			leader = leader.replace(",", " ");
			leader = leader.replace(":", " ");
			System.out.println(leader);
		}
		
		
		
		
		
//		TopicMetadata topicdata = (AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils));
//		List<kafka.javaapi.PartitionMetadata> partitionMetadatas = new kafka.javaapi.TopicMetadata(topicdata).partitionsMetadata();
//        int numPartitions = topicdata.partitionMetadata().size();
//        List<kafka.cluster.Partition> partitions = new ArrayList<>();
//		org.apache.kafka.common.requests.MetadataResponse.TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils);
//        System.out.println(numPartitions);
//        System.out.println(topicdata);
        
        
//        System.out.println(topicdata.partitionMetadata().get(0));

//        int numPartitions = metadata.partitionMetadata().size();

//        List<Partition> partitions = new ArrayList<>();
        
//        topicdata.partitionMetadata().stream().

//        topicdata.partitionMetadata().stream().forEach(p -> {
//            int partitionID = p.partition();
//            int leader = p.leader().id();
//
//            java.util.List<Integer> replicas = p.replicas().stream().map(n -> n.id()).collect(Collectors.toList());
//            List<Integer> isr = p.isr().stream().map(n -> n.id()).collect(Collectors.toList());
//
//            partitions.add(new Partition(partitionID, leader, replicas, isr));
//        });
//        
//
//        int replicationFactor = partitions.stream().mapToInt(r -> r.getReplicas().size()).max().getAsInt();
//
//
//        /* retrieve topic configuration*/
//        Map<String, String> configs = new HashMap<>();
//
//        Properties prop = AdminUtils.fetchEntityConfig(zkUtils, kafka.server.ConfigType.Topic(), topicName);
//        prop.stringPropertyNames().stream().forEach(k -> configs.put(k, prop.getProperty(k)));

//        return new Topic(topicName, numPartitions, replicationFactor, configs, partitions);
		   
	}
	
	public static void main(String[] args){

//		String topicName = "my-topic";
//		String[] options = new String[]{  
//			    "--describe",  
//			    "--zookeeper",  
//			    "localhost:2181",  
//			    "--topic",  
//			    "default",  
//			};  
//		options[4] = topicName;
//		
//			TopicCommand.main(options);  
		describeTopic();




		    
		    
		    

	}

}
