package com.alogic.xscript.kafka.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;


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
	
	protected String zookeeperConnector = "localhost:2181";
	protected String groupId = "test";
	protected String syncTimeMs = "200";
	protected String sessionTimeoutMs = "4000";
	protected String autoCommitIntervalMs = "1000";
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
		zookeeperConnector = PropertiesConstants.getString(p, "zookeeperConnector", zookeeperConnector);
		groupId = PropertiesConstants.getString(p, "groupId", groupId);
		syncTimeMs = PropertiesConstants.getString(p, "syncTimeMs", syncTimeMs);
		sessionTimeoutMs = PropertiesConstants.getString(p, "sessionTimeoutMs", sessionTimeoutMs);
		autoCommitIntervalMs = PropertiesConstants.getString(p, "autoCommitIntervalMs", autoCommitIntervalMs);
		autoOffsetReset = PropertiesConstants.getString(p, "autoOffsetReset", autoOffsetReset);
		serializerClass = PropertiesConstants.getString(p, "valueSerializer", serializerClass);
		
		props.put("zookeeper.connect", zookeeperConnector);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", sessionTimeoutMs);
		props.put("zookeeper.sync.time.ms", syncTimeMs);
	    props.put("auto.commit.interval.ms", autoCommitIntervalMs);
	    props.put("auto.offset.reset", autoOffsetReset);
	    props.put("serializer.class",serializerClass);
	    connect();
	     	
	}
	public ConsumerConnector(Properties p,String zookeeperconnect,String groupid,String synctimems,
			String sessiontimeoutms,String autocommitintervalms,String autooffsetreset,
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
		
		
		System.err.println("zookeeperConnector "+zookeeperConnector);
		System.err.println("groupId "+groupId);
		System.err.println("syncTimeMs "+syncTimeMs);
		System.err.println("sessionTimeoutMs "+sessionTimeoutMs);
		System.err.println("autoCommitIntervalMs "+autoCommitIntervalMs);
		System.err.println("autoOffsetReset "+autoOffsetReset);
		System.err.println("serializerClass "+serializerClass);
		
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
		System.err.println("zookeeperConnector "+props.getProperty("zookeeper.connect"));
		System.err.println("groupId "+props.getProperty("group.id"));
		System.err.println("syncTimeMs "+props.getProperty("zookeeper.session.timeout.ms"));
		System.err.println("sessionTimeoutMs "+props.getProperty("zookeeper.sync.time.ms"));
		System.err.println("autoCommitIntervalMs "+props.getProperty("auto.commit.interval.ms"));
		System.err.println("autoOffsetReset "+props.getProperty("auto.offset.reset"));
		System.err.println("serializerClass "+props.getProperty("serializer.class"));
		
		ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
//        try {
//			consumer.wait(timeout);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//consumer = com.alogic.xscript.kafka.util.KafkaUtil.getConsumer();
        
	
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
        long timeStart=System.currentTimeMillis() ;
        int i=0;
        while (it.hasNext())
        {
        	String msg = it.next().message().toString();
        	System.out.println(msg);
        	msglist.add(msg);
        	i++;
        	if(i>=2)
        	{
        		break;
        	}
        		
        	//long timeEnd = System.currentTimeMillis();
        	//if(timeEnd-timeStart>30000)
        	//{
        	//	break;
        	//}
        }
        
        	//consumer.shutdown();
           
        	return msglist;
        	
            
        
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
