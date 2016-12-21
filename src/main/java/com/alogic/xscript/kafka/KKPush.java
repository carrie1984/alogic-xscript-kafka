package com.alogic.xscript.kafka;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.ConsumerConnector;
import com.alogic.xscript.plugins.Segment;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;
import com.jayway.jsonpath.spi.JsonProvider;
import com.jayway.jsonpath.spi.JsonProviderFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KKPush extends Segment{

	public KKPush(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	
	private String pid = "$mq-pusher";
	protected int thread = 1;
	protected String topic = "test";
	protected String tag = "data";
	protected long waittime = 2000;
	
	
	// onExecute函数的参数
	protected Map<String, Object> rootPara;
	protected Map<String, Object> currentPara;
	protected LogicletContext ctxPara;
	protected ExecuteWatcher watcherPara;

	
	/*
	 * 返回结果的id
	 */
	protected String id;
	
	public void configure(Properties p){
		super.configure(p);
		pid = PropertiesConstants.getString(p,"pid", pid);
		id = PropertiesConstants.getString(p,"id", "$" + getXmlTag());
		thread = PropertiesConstants.getInt(p, "thread",thread);
		topic = PropertiesConstants.getRaw(p, "topic", topic);
		waittime = PropertiesConstants.getLong(p, "waittime", waittime);
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		ConsumerConnector conn = ctx.getObject(pid);
		rootPara = root;
		currentPara = current;
		ctxPara = ctx;
		watcherPara = watcher;
		
		recvPushMsg(conn,topic, thread,waittime);
	}
	
	public void recvPushMsg(ConsumerConnector conn,String topic,int thread,long waittime)
	{
	
		kafka.javaapi.consumer.ConsumerConnector consumer = conn.getPushConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, thread);

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = 
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<String, String> it = stream.iterator();

        long begintime = System.currentTimeMillis();
        while (((System.currentTimeMillis()-begintime)<=waittime)&&it.hasNext())
        {
        	
        	String msg = it.next().message().toString();
        	ctxPara.SetValue(id,msg);
        	System.out.println(msg);
        	super.onExecute(rootPara, currentPara, ctxPara, watcherPara);
			JsonProvider provider = JsonProviderFactory.createProvider();
			System.out.println(provider.toJson(rootPara));
        }

    }


}
