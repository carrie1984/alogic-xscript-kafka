package com.alogic.xscript.kafka;

import java.util.Arrays;
import java.util.Properties;

import javax.print.DocFlavor.STRING;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.alogic.xscript.kafka.util.KafkaUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;

//kafka-console-consumer.bat --zookeeper localhost:2181 --topic test
public class KKConsumer {
	public static KafkaConsumer<String, String> CreateConsumer()
	{
		KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();
		return consumer;
	}
	public static void main(String [] args)
	{
//		 Properties props = new Properties();
//	     props.put("bootstrap.servers", "localhost:9092");
//	     props.put("group.id", "test");
//	     props.put("enable.auto.commit", "true");
//	     props.put("auto.commit.interval.ms", "1000");
//	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		 String topicName = "test";
		 KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();
	     consumer.subscribe(Arrays.asList(topicName));
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	     }
	}

}
