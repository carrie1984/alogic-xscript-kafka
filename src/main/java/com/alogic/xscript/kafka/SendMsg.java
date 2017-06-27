package com.alogic.xscript.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.doc.XsObject;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;
import com.google.gson.Gson;


public class SendMsg extends AbstractLogiclet{
	
	protected String acctId = "";
	protected String type = "";
	protected String topic = "";
	protected String bootstrapServers = "";
	protected static String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
	protected static String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
	public SendMsg(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	public void configure(Properties p) {
		super.configure(p);

		acctId = PropertiesConstants.getRaw(p, "acctId", acctId);
		type = PropertiesConstants.getRaw(p, "type", type);
		topic = PropertiesConstants.getRaw(p, "topic", topic);
		bootstrapServers = PropertiesConstants.getRaw(p, "bootstrapServers", bootstrapServers);
		}
	
	public static void sendMsg(String acctid,String type,String bootstrapservers,String topic)
	{
		java.util.Properties props = new java.util.Properties();
		props.put("bootstrap.servers", bootstrapservers);
		props.put("key.serializer",keySerializer);
		props.put("value.serializer", valueSerializer);
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		Map<String, String> msg = new HashMap<>();
		msg.put("acctId", acctid);
		msg.put("type", type);
		Gson msgGson = new Gson();
		String msgjson = msgGson.toJson(msg);
		System.err.println(msgjson);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,msgjson);
		
		producer.send(record);

		producer.close();
	}

	@Override
	protected void onExecute(XsObject root, XsObject current, LogicletContext ctx,
			ExecuteWatcher watcher){
		String acctIdData = ctx.transform(acctId);
		String typeData = ctx.transform(type);
		String topicData = ctx.transform(topic);
		String bootstrapServersData = ctx.transform(bootstrapServers);
		
		sendMsg(acctIdData, typeData, bootstrapServersData, topicData);
		
		
	}

}
