package com.alogic.xscript.kafka.admin;

import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.KKConnector;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class KKCreateTopic extends KKAdminOperation{
	
	/**
	 * a logger of log4j
	 */
	protected static Logger logger = LogManager.getLogger(KKCreateTopic.class);
	
	
	protected String topic = "";
	protected int partition = 1;
	protected int replication = 1;
	protected String topicConfigpara = ""; 
	protected String topicConfignum = "";
	

	public KKCreateTopic(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void configure(Properties p)
	{
		super.configure(p);
		topic = PropertiesConstants.getRaw(p, "topic", topic);
		partition = PropertiesConstants.getInt(p, "partition", partition);
		replication = PropertiesConstants.getInt(p, "replication", replication);
		topicConfigpara = PropertiesConstants.getRaw(p, "topicConfigpara", topicConfigpara);
		topicConfignum = PropertiesConstants.getRaw(p, "topicConfignum", topicConfignum);
	}

	@Override
	protected void onExecute(KKConnector row, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		// TODO Auto-generated method stub
		java.util.Properties topicConfig = new java.util.Properties(); // add per-topic configurations settings here
	    //topicConfig.setProperty(topicConfigpara, topicConfignum);

	    row.createTopic(topic,partition,replication,topicConfig);
	    
	}

}
