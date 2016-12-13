package com.alogic.xscript.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

import com.alogic.xscript.kafka.util.KafkaUtil;

/*
 * prod-conn实现类
 * 
 * @author cuijialing
 */

//kafka-console-producer.bat --broker-list localhost:9092 --topic test
public class KKProducer extends Segment {
	
	protected String cid = "prod-conn";
	public KKProducer(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
		registerModule("send", KKSendMsg.class);
	}

	public static  KafkaProducer< String, String> CreateProducer()
	{
		 KafkaProducer<String, String> producer = KafkaUtil.getProducer();
		 return producer;
	}

	public static void main(String [] args)
	{
//		Properties props = new Properties();
//		 props.put("bootstrap.servers", "localhost:9092");
//		 props.put("acks", "all");
//		 props.put("retries", 0);
//		 props.put("batch.size", 16384);
//		 props.put("linger.ms", 1);
//		 props.put("buffer.memory", 33554432);
//		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		 Producer<String, String> producer = new KafkaProducer<>(props);		 
		 String topicName = "test";
		 KafkaProducer<String, String> producer = KafkaUtil.getProducer();

		 for(int i = 0; i < 100; i++)
		     producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));

		 producer.close();
	}

}