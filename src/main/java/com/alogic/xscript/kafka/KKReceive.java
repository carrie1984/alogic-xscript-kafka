package com.alogic.xscript.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.ConsumerConnector;
import com.alogic.xscript.kafka.util.ProducerConnector;
import com.alogic.xscript.plugins.Segment;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

import kafka.consumer.ConsumerConfig;

public class KKReceive extends Segment{

	public KKReceive(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	
	private String pid = "$consumer-conn";
	protected int thread = 1;
	protected String topic = "test";
	protected String tag = "data";
	
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
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		ConsumerConnector consumer = ctx.getObject(pid);
		//============================
		
        //===========================================================================
		List<String> result = new ArrayList<>();
		result = consumer.recvMsg(topic, thread);
		root.put(tag, result);
//		if (result.size() > 0){
//			for (String value:result){
//				ctx.SetValue(id, value);
//				super.onExecute(root, current, ctx, watcher);
//			}
//		
//	}
	}

}
