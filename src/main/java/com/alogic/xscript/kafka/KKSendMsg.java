package com.alogic.xscript.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.alogic.xscript.kafka.util.KafkaUtil;

import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

//send masseage once
public class KKSendMsg extends AbstractLogiclet {
	public KKSendMsg(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	protected String pid = "$prod-conn";
	protected String id;
	
	protected String data;
	protected String topic;
	
	@Override
	public void configure(Properties p)
	{
		super.configure(p);
		pid = PropertiesConstants.getString(p, "pid", pid, true);
		id = PropertiesConstants.getString(p, "id", "$"+getXmlTag(), true);
		topic = PropertiesConstants.getString(p, "topic", "", true);
		data = PropertiesConstants.getString(p, "data", "", true);		
	}
	@Override
	protected void onExecute(Map<String, Object> root, Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		// TODO Auto-generated method stub
		
	}
	
	
	
	
	
	
	public static void SendMsg() throws Exception
	{
		String topicName = "test";
		KafkaProducer<String, String> producer = KKProducer.CreateProducer();
//		for(int i = 0; i < 100; i++)
//		     producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
//
//		 producer.close();
		int i = 0;
		while(true) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, String.valueOf(i), "this is message"+i);
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null)
						e.printStackTrace();
					System.out.println("message send to partition " + metadata.partition() + ", offset: " + metadata.offset());
				}
			});
			i++;
			Thread.sleep(1000);
		}
	}
	public static void main(String[] args)
	{
		String topicName = "test";
		String msg = "bilibili";
		KafkaProducer<String, String> producer = KafkaUtil.getProducer();
//		producer.send(new ProducerRecord<String, String>(topicName, msg));
		for(int i = 0; i < 100; i++)
		     producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), msg));

		 producer.close();
	}


}
