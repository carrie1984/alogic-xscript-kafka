package com.alogic.xscript.kafka.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
//log
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

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
	
	public static void main(String[] args)
	{
		consumer = getConsumer();
		//kProducer = getProducer();
		
		//ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "1", "hhh");
		//kProducer.send(record);
		List<String> msglist = new ArrayList<>();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("test",1);

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = 
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get("test").get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext())
        {
        	System.out.println(it.next().message());
        	//msglist.add(it.next().message().toString());
        }
        
		//System.out.println("====="+kProducer.send(record).isDone());
		
		}

}
