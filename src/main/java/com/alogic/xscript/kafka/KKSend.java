package com.alogic.xscript.kafka;

import java.util.Map;

import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

import com.alogic.xscript.kafka.util.ProducerConnector;

/*
 * 生产者发送消息
 * @author cuijialing
 */

public class KKSend extends AbstractLogiclet{
	public KKSend(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}

	/*
	 * producer的cid
	 */
	private String pid = "$mq-sender";
	
	protected String topic = "test";
	protected String key = "test";
	protected String value = "test-msg";
	protected String tag = "data";
	/*
	 * 返回结果的id
	 */
	protected String id;
	
	public void configure(Properties p){
		super.configure(p);
		pid = PropertiesConstants.getString(p,"pid", pid);
		id = PropertiesConstants.getString(p,"id", "$" + getXmlTag());
		topic = PropertiesConstants.getString(p, "topic", "");
		key = PropertiesConstants.getString(p, "key", "");
		value = PropertiesConstants.getString(p, "value", "");
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		ProducerConnector producer = ctx.getObject(pid);
		String result = "";
		//result = producer.sendMsg(topic,key,value).toString();
		result = producer.sendMsg(topic,key,value).toString();
		root.put(tag, result);
	}


}
