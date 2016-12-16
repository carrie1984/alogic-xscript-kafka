package com.alogic.xscript.kafka.util;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/*
 * kafka pull方法的消费者连接类
 * 注意本方法适用于打开生产者传数据，然后关闭生产者
 * 再启动消费者将之前的数据拉下
 * 需要设置polltimems
 */

public class ConsuPullConn {
	/*
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(ConsuPullConn.class);
	/*
	 * kafka pull consumer 连接参数
	 */
	
	protected String bootstrapServer = "localhost:9092";
	protected String groupId = "test";
	protected String enableAutoCommit = "true";
	protected String autoCommitIntervalMs = "1000";
	protected String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
	protected String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
	//连接的属性参数容器
	 public static java.util.Properties props = new java.util.Properties();
	 
    /*
     * consumer
     */
	protected static KafkaConsumer<String, String> consumer;
	public ConsuPullConn(Properties p)
	{
		bootstrapServer = PropertiesConstants.getString(p, "bootstrapServer", bootstrapServer);
		groupId = PropertiesConstants.getString(p, "groupId", groupId);
		enableAutoCommit = PropertiesConstants.getString(p, "enableAutoCommit", enableAutoCommit);
		autoCommitIntervalMs = PropertiesConstants.getString(p, "autoCommitIntervalMs", autoCommitIntervalMs);
		keyDeserializer = PropertiesConstants.getString(p, "keyDeserializer", keyDeserializer);
		valueDeserializer = PropertiesConstants.getString(p, "valueDeserializer", valueDeserializer);
		
		props.put("bootstrap.servers", bootstrapServer);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", enableAutoCommit);
		props.put("auto.commit.interval.ms", autoCommitIntervalMs);
		props.put("key.deserializer", keyDeserializer);
		props.put("value.deserializer", valueDeserializer);
		connect();
	}
	public ConsuPullConn(Properties p,String bootstrapserver,String groupid,
			String enableautocommit,String autocommitintervalms,String keydeserializer,String valuedeserializer)
	{
		bootstrapServer = bootstrapserver;
		groupId = groupid;
		enableAutoCommit = enableautocommit;
		autoCommitIntervalMs = autocommitintervalms;
		keyDeserializer = keydeserializer;
		valueDeserializer = valuedeserializer;
		
		props.put("bootstrap.servers", bootstrapServer);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", enableAutoCommit);
		props.put("auto.commit.interval.ms", autoCommitIntervalMs);
		props.put("key.deserializer", keyDeserializer);
		props.put("value.deserializer", valueDeserializer);
		connect();
	}
	/*
	 * 连接到consumer
	 */
	
	public void connect()
	{
		consumer = new KafkaConsumer<>(props);
	}
	/*
	 * 关闭consumer
	 */
	public void disconnect()
	{
		consumer.close();
	}
	public void reconnect()
	{
		disconnect();
		connect();
	}
	/*
	 * 消费者接收消息
	 */
	public List<String> recvMsg(String topic,int polltime)
	{
		List<String> msglist = new ArrayList<>();
		consumer.subscribe(Arrays.asList(topic));
		ConsumerRecords<String, String> records = consumer.poll(1000);
		int index = 0;
        for (ConsumerRecord<String, String> record : records)
        {
        	System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        	msglist.add(index, record.value());
        	index++;
        }
		return msglist;
	}
	
}
