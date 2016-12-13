package com.alogic.xscript.kafka.util;

import java.util.Properties;
//log
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
/*
 * kafka工具类，kafka暂时不需要显式的进行连接
 */

public class KafkaUtil {

	/**
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(KafkaUtil.class);
	
	private static KafkaProducer<String, String> kProducer;
	private static KafkaConsumer<String, String> kConsumer;
	
	public static KafkaProducer<String, String> getProducer()
	{
		if(kProducer==null)
		{
			Properties props = new Properties();
			props.put("bootstrap.servers", "localhost:9092");
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			
			kProducer = new KafkaProducer<String,String>(props);
			
		}
		return kProducer;
	}
	public static KafkaConsumer<String, String> getConsumer()
	{
		if(kConsumer==null)
		{
			 Properties props = new Properties();
		     props.put("bootstrap.servers", "localhost:9092");
		     props.put("group.id", "test");
		     props.put("enable.auto.commit", "true");
		     props.put("auto.commit.interval.ms", "1000");
		     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		     kConsumer = new KafkaConsumer<>(props);
		}
		return kConsumer;
	}

}
