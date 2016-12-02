package com.alogic.xscript.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.alogic.xscript.kafka.util.KafkaUtil;

import scala.annotation.implicitNotFound;


//receive message once,with n as number of message 
public class KKRecvMsg {
	public static void RecvMsg()
	{
		KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();
		String topicName = "test";
		int number = 5;
		consumer.subscribe(Arrays.asList(topicName));
		int flag = 0;
		while(flag<number)
		{
			ConsumerRecords<String, String> records = consumer.poll(0);
			for(ConsumerRecord<String, String> record : records) {
				System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
			}
			flag++;
		}
		
	}
	public static void main(String[] args)
	{
		KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();
		String topicName = "test";
		int number = 5;
		int flag = 0;
		consumer.subscribe(Arrays.asList(topicName));
		//为了控制接收消息的数目，尝试使用接收全部的消息，但是最终只将指定数量的消息显示在控制台
		while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         
	         for (ConsumerRecord<String, String> record : records)
	         {
	        	 flag++;
	        	 if(flag==number+1)
	        	 {
	        		 break;
	        	 }
	        	 System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	         }
	             
	     }
		

		//以下方法因为一次性可能就把所有消息都poll下来了，难以控制消息条数
//		int flag = 0;
//		while(flag<number)
//		{
//			ConsumerRecords<String, String> records = consumer.poll(100);
//			for(ConsumerRecord<String, String> record : records) {
//				System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
//			}
//			flag++;
//		}
		
	}
}
