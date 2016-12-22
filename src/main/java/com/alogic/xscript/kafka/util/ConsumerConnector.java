package com.alogic.xscript.kafka.util;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

import kafka.consumer.ConsumerConfig;


/*
 * kafka实现push模式的消费者连接类
 * 本方法适用于设置waittime时间来实时接收
 * 生产者发送的消息
 * @author cuijialing
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
	
	public kafka.javaapi.consumer.ConsumerConnector getPushConsumer()
	{
		return consumer;		
	}
	
	/*
	 * 消费者使用push模式接受消息的方法在KKPush中实现
	 */
	
}
