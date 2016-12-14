package com.alogic.xscript.kafka.util;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;


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
	protected String bootstrapServers = "$bootstrapServrrs";
	protected String groupId = "";
	protected String enableAutoCommit = "true";
	protected int autoCommitIntervalMs = 1000;
	protected String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
	protected String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
	
	//连接的属性参数容器
	public static java.util.Properties props = new java.util.Properties();
	
	/*
	 * consumer
	 */
	protected static KafkaConsumer<String, String> consumer = null;
	
	public ConsumerConnector(Properties p)
	{
		bootstrapServers = PropertiesConstants.getRaw(p, "bootstrapServers", bootstrapServers);
		groupId = PropertiesConstants.getRaw(p, "groupId", groupId);
		enableAutoCommit = PropertiesConstants.getRaw(p, "enableAutoCommit", enableAutoCommit);
		autoCommitIntervalMs = PropertiesConstants.getInt(p, "autoCommitIntervalMs", autoCommitIntervalMs);
		keySerializer = PropertiesConstants.getRaw(p, "keySerializer", keySerializer);
		valueSerializer = PropertiesConstants.getRaw(p, "valueSerializer", valueSerializer);
		
		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", enableAutoCommit);
	    props.put("auto.commit.interval.ms", autoCommitIntervalMs);
	    props.put("key.deserializer", keySerializer);
	    props.put("value.deserializer",valueSerializer);
	     	
	}
	public ConsumerConnector(Properties p,String bootstrapservers,String groupid,
			String enableautocommit,int autocommitintervalms,String keyserializer,
			String valueserializer
			)
	{
		bootstrapServers = bootstrapservers;
		groupId = groupid;
		enableAutoCommit = enableautocommit;
		autoCommitIntervalMs = autocommitintervalms;
		keySerializer = keyserializer;
		valueSerializer = valueserializer;
		
		
		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", enableAutoCommit);
	    props.put("auto.commit.interval.ms", autoCommitIntervalMs);
	    props.put("key.deserializer", keySerializer);
	    props.put("value.deserializer", valueSerializer);
	     	
	}	
	
	/*
	 * 连接到consumer
	 */
	public void connect()
	{
		if(consumer==null)
		{
			consumer = new KafkaConsumer<>(props);
		}
	}
	/*
	 * 关闭consumer
	 */
	public void disconnect()
	{
		if(isConnected())
		{
			consumer.close();
			consumer = null;
		}
	}
	
	public boolean isConnected()
	{
		if(consumer!=null)
		{
			return true;
		}
		else{
			return false;
		}
	}
	
	public void reconnect()
	{
		disconnect();
		connect();
	}
	
	/*
	 * 消费者接受消息
	 */
	
	public void recvMsg(String topic,int polltimems)
	{
		consumer.subscribe(Arrays.asList(topic));
		while(true)
		{
			ConsumerRecords<String, String> records = consumer.poll(polltimems);
			for(ConsumerRecord<String, String> record : records)
			{
				System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
