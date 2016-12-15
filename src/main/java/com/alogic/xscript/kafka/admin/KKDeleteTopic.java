package com.alogic.xscript.kafka.admin;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.KKConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/*
 * 删除topic
 * @author cuijialing
 */
public class KKDeleteTopic extends KKAdminOperation{
	
	protected String topic = "";

	public KKDeleteTopic(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void configure(Properties p) 
	{
		super.configure(p);
		topic  = PropertiesConstants.getRaw(p, "topic",topic);
	}

	@Override
	protected void onExecute(KKConnector row, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		// TODO Auto-generated method stub
		row.deleteTopic(topic);
		System.err.println(topic+" is delete");
		
	}
	
	
	
	
}
