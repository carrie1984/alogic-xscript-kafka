package com.alogic.xscript.kafka.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import scala.collection.generic.BitOperations.Int;


/*
 * kafka的消费者连接类
 */
public class ConsumerConnector {
	/*
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(ConsumerConnector.class);
	
	/*
	 * consumer 连接参数
	 */
	//Kafka集群连接串，可以由多个host:port组成
	
	protected String zookeeperConnector = "$zookeeperConnector";
	protected String groupId = "test";
	protected int syncTimeMs = 200;
	protected int sessionTimeoutMs = 4000;
	protected int autoCommitIntervalMs = 1000;
	protected String autoOffsetReset = "smallest";
	protected String serializerClass = "kafka.serializer.StringEncoder";
	
	
	
	//连接的属性参数容器
	public static java.util.Properties props = new java.util.Properties();
	
	/*
	 * consumer
	 */;
	protected static kafka.javaapi.consumer.ConsumerConnector consumer;
	
	public ConsumerConnector(Properties p)
	{
		zookeeperConnector = PropertiesConstants.getRaw(p, "zookeeperConnector", zookeeperConnector);
		groupId = PropertiesConstants.getRaw(p, "groupId", groupId);
		syncTimeMs = PropertiesConstants.getInt(p, "syncTimeMs", syncTimeMs);
		sessionTimeoutMs = PropertiesConstants.getInt(p, "sessionTimeoutMs", sessionTimeoutMs);
		autoCommitIntervalMs = PropertiesConstants.getInt(p, "autoCommitIntervalMs", autoCommitIntervalMs);
		autoOffsetReset = PropertiesConstants.getRaw(p, "autoOffsetReset", autoOffsetReset);
		serializerClass = PropertiesConstants.getRaw(p, "valueSerializer", serializerClass);
		
		props.put("zookeeper.connect", zookeeperConnector);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", sessionTimeoutMs);
		props.put("zookeeper.sync.time.ms", syncTimeMs);
	    props.put("auto.commit.interval.ms", autoCommitIntervalMs);
	    props.put("auto.offset.reset", autoOffsetReset);
	    props.put("serializer.class",serializerClass);
	    connect();
	     	
	}
	public ConsumerConnector(Properties p,String zookeeperconnect,String groupid,int synctimems,
			int sessiontimeoutms,int autocommitintervalms,String autooffsetreset,
			String serializerclass
			)
	{
		zookeeperConnector = zookeeperconnect;
		groupId = groupid;
		syncTimeMs = synctimems;
		sessionTimeoutMs = sessiontimeoutms;
		autoCommitIntervalMs = autocommitintervalms;
		autoOffsetReset = autooffsetreset;
		serializerClass = serializerclass;
		
		
		props.put("zookeeper.connect", zookeeperConnector);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", sessionTimeoutMs);
		props.put("zookeeper.sync.time.ms", syncTimeMs);
	    props.put("auto.commit.interval.ms", autoCommitIntervalMs);
	    props.put("auto.offset.reset", autoOffsetReset);
	    props.put("serializer.class",serializerClass);
	    
	    connect();
	     	
	     	
	}	
	
	/*
	 * 连接到consumer
	 */
	public void connect()
	{
		
			ConsumerConfig config = new ConsumerConfig(props);

	        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
//		java.util.Properties props = new java.util.Properties();
        //zookeeper 配置
//        props.put("zookeeper.connect", "127.0.0.1:2181");
//
//        //group 代表一个消费组
//        props.put("group.id", "test");
//
//        //zk连接超时
//        props.put("zookeeper.session.timeout.ms", "4000");
//        props.put("zookeeper.sync.time.ms", "200");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.offset.reset", "smallest");
//        //序列化类
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//
//        ConsumerConfig config = new ConsumerConfig(props);
//
//        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	
	}
	/*
	 * 关闭consumer
	 */
	public void disconnect()
	{
		
			consumer.shutdown();
		
	}
	

	
	public void reconnect()
	{
		disconnect();
		connect();
	}
	
	/*
	 * 消费者接受消息
	 */
	
	public List<String> recvMsg(String topic,int thread)
	{
		
		List<String> msglist = new ArrayList<>();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, thread);

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = 
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext())
        {
        	//System.out.println(it.next().message());
        	msglist.add(it.next().message().toString());
        }
        	return msglist;
            
        
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
