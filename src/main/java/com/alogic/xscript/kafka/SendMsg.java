package com.alogic.xscript.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.doc.XsObject;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;


public class SendMsg extends AbstractLogiclet{
	
	protected String msg = "";
	protected String topic = "";
	protected String key = "";
	protected String bootstrapServers = "";
	protected static String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
	protected static String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
	public SendMsg(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	public void configure(Properties p) {
		super.configure(p);

		msg = PropertiesConstants.getRaw(p, "msg", msg);
		topic = PropertiesConstants.getRaw(p, "topic", topic);
		key = PropertiesConstants.getRaw(p, "key", key);
		bootstrapServers = PropertiesConstants.getRaw(p, "bootstrapServers", bootstrapServers);
		}
	
	public static void sendMsg(String bootstrapservers,String topic,String key,String msg)
	{
		java.util.Properties props = new java.util.Properties();
		props.put("bootstrap.servers", bootstrapservers);
		props.put("key.serializer",keySerializer);
		props.put("value.serializer", valueSerializer);
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,msg);
		
		producer.send(record);

		producer.close();
	}
	
	
	@Override
	protected void onExecute(XsObject root, XsObject current, LogicletContext ctx,
			ExecuteWatcher watcher){

		String topicData = ctx.transform(topic);
		String keyData = ctx.transform(key);

		String bootstrapServersData = ctx.transform(bootstrapServers);
		String msgData = ctx.transform(msg);
		
		sendMsg(bootstrapServersData, topicData, keyData, msgData);
		
		
		
		
	}

}
