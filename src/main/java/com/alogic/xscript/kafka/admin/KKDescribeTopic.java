package com.alogic.xscript.kafka.admin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.KKConnector;
import com.alogic.xscript.kafka.util.TopicInfo;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;
import com.google.gson.Gson;

public class KKDescribeTopic extends KKAdminOperation{
	
	protected String topic = "";
	protected String tag = "data";

	public KKDescribeTopic(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	@Override
	public void configure(Properties p)
	{
		super.configure(p);
		topic = PropertiesConstants.getRaw(p, "topic", topic);
		tag = PropertiesConstants.getString(p, "tag", tag);
		System.out.println(topic+"=============");
	}

	@Override
	protected void onExecute(KKConnector row,Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		// TODO Auto-generated method stub
		String topicValue = ctx.transform(topic).trim();
		TopicInfo temp = new TopicInfo();
		
		HashMap<String, Object> AllInfo = temp.getTopicInfo(row, topicValue);
	
		root.put(tag, AllInfo);
		
	}
	


}
