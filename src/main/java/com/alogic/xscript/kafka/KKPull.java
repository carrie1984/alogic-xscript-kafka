package com.alogic.xscript.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.ConsuPullConn;
import com.alogic.xscript.plugins.Segment;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class KKPull extends Segment{
	/*
	 * consumer的pid
	 */
	protected String pid = "$mq-puller";
	/*
	 * 返回数据的标签
	 */
	protected String tag = "data";
	/*
	 * 返回结果的id
	 */
	protected String id;
	protected String topic = "test";
	protected int pollTimeMs = 100;

	public KKPull(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	public void configure(Properties p)
	{
		super.configure(p);
		pid = PropertiesConstants.getString(p,"pid", pid);
		id = PropertiesConstants.getString(p,"id", "$" + getXmlTag());
		pollTimeMs = PropertiesConstants.getInt(p, "pollTimeMs",pollTimeMs);
		topic = PropertiesConstants.getRaw(p, "topic", topic);
	}
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		ConsuPullConn consumer = ctx.getObject(pid);
		//============================
		
        //===========================================================================
		List<String> result = new ArrayList<>();
		result = consumer.recvMsg(topic, pollTimeMs);
		System.err.println("======pull==============");
		root.put(tag, result);
		if (result.size() > 0){
			for (String value:result){
				ctx.SetValue(id, value);
				super.onExecute(root, current, ctx, watcher);
			}
		
	}
	}	

}
