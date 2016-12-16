package com.alogic.xscript.kafka.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

//log
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
/*
 * kafka工具类，kafka暂时不需要显式的进行连接
 */
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaUtil {

	/**
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(KafkaUtil.class);
	
	private static KafkaProducer<String, String> kProducer;
	private static kafka.javaapi.consumer.ConsumerConnector consumer;

	
	public static KafkaProducer<String, String> getProducer()
	{
		if(kProducer==null)
		{
			Properties props = new Properties();
			props.put("bootstrap.servers", "localhost:9092");
			props.put("request.required.acks","-1");
//			props.put("acks", "all");
//			props.put("retries", 0);
//			props.put("batch.size", 16384);
//			props.put("linger.ms", 1);
//			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			
			kProducer = new KafkaProducer<String,String>(props);
			
		}
		return kProducer;
	}
	public static ConsumerConnector getConsumer()
	{
		Properties props = new Properties();
        //zookeeper 配置
        props.put("zookeeper.connect", "127.0.0.1:2181");

        //group 代表一个消费组
        props.put("group.id", "test");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        return consumer;
		
	}
	public static void pollconsumer()
	{
		 Properties props = new Properties();
	     props.put("bootstrap.servers", "localhost:9092");
	     props.put("group.id", "test");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		 String topicName = "test";
		 
	     consumer.subscribe(Arrays.asList(topicName));
	    // while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(1000);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	     //}
	         consumer.close();
	}
	
	public static void main(String[] args)
	{
		//consumer = getConsumer();
		

		kProducer = getProducer();
		System.out.println(kProducer.toString());
		ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("test", "1", "20161216 h1");
		try {
			kProducer.send(record1).get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("test", "1", "20161216 h2");
		try {
			kProducer.send(record2).get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ProducerRecord<String, String> record3 = new ProducerRecord<String, String>("test", "1", "20161216 h3");
		try {
			kProducer.send(record3).get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ProducerRecord<String, String> record4 = new ProducerRecord<String, String>("test", "1", "20161216 h4");
		try {
			kProducer.send(record4).get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		kProducer.close();
		pollconsumer();
//		List<String> msglist = new ArrayList<>();
//		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//        topicCountMap.put("test",1);
//
//        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
//        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
//
//        Map<String, List<KafkaStream<String, String>>> consumerMap = 
//                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
//        KafkaStream<String, String> stream = consumerMap.get("test").get(0);
//        ConsumerIterator<String, String> it = stream.iterator();
//        while (it.hasNext())
//       {
//        	System.out.println(it.next().message());
//        	//msglist.add(it.next().message().toString());
//        }
        
		//System.out.println("====="+kProducer.send(record).isDone());
		
		}

}
