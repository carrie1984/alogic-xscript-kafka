package com.alogic.xscript.kafka.util;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/*
 * kafka的生产者连接类
 * @author cuijialing
 */
public class ProducerConnector {
	
	/*
	 * a logger of slf4j
	 */
	protected static final Logger logger = LoggerFactory.getLogger(ProducerConnector.class);
	
	
	/*
	 * producer的连接参数
	 */
	//Kafka集群连接串，可以由多个host:port组成
	protected String bootstrapServers = "$bootstrapServrrs";
	//broker消息确认的模式,默认为1
	protected String acks = "1";
	//发送失败时Producer端的重试次数，默认为0
	protected String retries = "0";
	protected String batchSize = "16384";
	protected String lingerMs = "1";
	protected String bufferMemory = "33554432";
	protected String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
	protected String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
	//连接的属性参数容器
	public static java.util.Properties props = new java.util.Properties();
	/*
	 * producer
	 */
	protected static KafkaProducer<String, String> producer = null;
	
	
	public ProducerConnector(Properties p)
	{
		bootstrapServers = PropertiesConstants.getRaw(p, "bootstrapServers", bootstrapServers);
		acks = PropertiesConstants.getRaw(p, "acks", acks);
		retries = PropertiesConstants.getString(p, "retries", retries);
		batchSize = PropertiesConstants.getString(p, "batchSize", batchSize);
		lingerMs = PropertiesConstants.getString(p, "lingerMs", lingerMs);
		bufferMemory = PropertiesConstants.getString(p, "bufferMemory", bufferMemory);
		keySerializer = PropertiesConstants.getRaw(p, "keySerializer", keySerializer);
		valueSerializer = PropertiesConstants.getRaw(p, "valueSerializer", valueSerializer);
		
		props.put("bootstrap.servers",bootstrapServers );
		props.put("acks", acks);
		props.put("retries", retries);
		props.put("batch.size", batchSize);
		props.put("linger.ms", lingerMs);
		props.put("buffer.memory", bufferMemory);
		props.put("key.serializer", keySerializer);
		props.put("value.serializer", valueSerializer);
		
		connect();
	}
	public ProducerConnector(Properties p,String bootstrapservers,
			String ackss,String retriess,String batchsize,String lingerms,
			String buffermemory,String keyserializer,String valueserializer
			)
	{
		bootstrapServers = bootstrapservers;
		acks = ackss;
		retries = retriess;
		batchSize = batchsize;
		lingerMs = lingerms;
		bufferMemory = buffermemory;
		keySerializer = keyserializer;
		valueSerializer = valueserializer;
		
		props.put("bootstrap.servers",bootstrapServers );
		props.put("acks", acks);
		props.put("retries", retries);
		props.put("batch.size", batchSize);
		props.put("linger.ms", lingerMs);
		props.put("buffer.memory", bufferMemory);
		props.put("key.serializer", keySerializer);
		props.put("value.serializer", valueSerializer);
		
		connect();
	} 
	
	/*
	 * 连接connector
	 */
	
	public void connect()
	{
		System.err.println("bootstrap.servers  "+props.getProperty("bootstrap.servers"));
		System.err.println("acks  "+props.getProperty("acks"));
		System.err.println("retries  "+props.getProperty("retries"));
		System.err.println("batch.size  "+props.getProperty("batch.size"));
		System.err.println("linger.ms  "+props.getProperty("linger.ms"));
		System.err.println("buffer.memory  "+props.getProperty("buffer.memory"));
		System.err.println("key.serializer  "+props.getProperty("key.serializer"));
		System.err.println("value.serializer  "+props.getProperty("value.serializer"));
		
		if(producer==null)
		{
			producer = new KafkaProducer<>(props);
		}
	}
	/*
	 * 关闭connector
	 */
	public void disconnect()
	{
		if(isConnected())
		{
			producer.close();
			producer = null;
		}
	}
	
	/*
	 * 是否已经连接
	 * @return true|false
	 */
	public boolean isConnected()
	{
		if(producer!=null)
		{
			return true;
		}
		else {
			return false;
		}	
	}
	/*
	 * 重新连接
	 */
	public void reconnect()
	{
		disconnect();
		connect();
	}
	
	/*
	 * 生产者发送消息
	 * @param topic
	 * @param key
	 * @param value 
	 */
	
	public String sendMsg(String topic,String key,String value)
	{
		if(!isConnected())
		{
			
			logger.error("the producer is not connected");
		}
		

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
		Future<RecordMetadata> sendresult = producer.send(record);
		if(sendresult.toString().isEmpty())
		{
			return "fail";
		}
		else{
			return String.valueOf(sendresult.toString());
		}
		
			
	}	

}
