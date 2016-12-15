package com.alogic.xscript.kafka.admin;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.KKConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class KKAlterTopic extends KKAdminOperation{
	
	protected String topic = "";
	protected String param = "";

	public KKAlterTopic(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void configure(Properties p)
	{
		super.configure(p);
		topic = PropertiesConstants.getRaw(p, "topic", topic);
		param = PropertiesConstants.getRaw(p, "param", param);
		
	}

	@Override
	protected void onExecute(KKConnector row, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		// TODO Auto-generated method stub
		System.err.println("======alter=========");
		row.alterTopic(topic, param);
		System.err.println(topic+" is altered");
		
	}

}
