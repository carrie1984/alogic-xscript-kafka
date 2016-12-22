package com.alogic.xscript.kafka.admin;

import java.util.List;
import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.KKConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/*
 * 列出当前的topic列表
 * @author cuijialing
 */
public class KKListTopic extends KKAdminOperation{
	
	protected String tag = "data";//json根目录标签

	public KKListTopic(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	@Override
	public void configure(Properties p) 
	{
		super.configure(p);
		tag = PropertiesConstants.getString(p, "tag", tag);
	}

	@Override
	protected void onExecute(KKConnector row,Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		
		List<String> topiclist = row.ListTopic();	
		root.put(tag, topiclist);
		
	}


}
