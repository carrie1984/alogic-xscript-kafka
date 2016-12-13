package com.alogic.xscript.kafka.admin;

import java.util.HashMap;
import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.KKConnector;
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
		topic = PropertiesConstants.getRaw(p, "topic", topic);
	}

	@Override
	protected void onExecute(KKConnector row, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		// TODO Auto-generated method stub
		Gson gson = new Gson();
		KKConnector.describeTopic(topic);
		//以下是将结果转换为json格式的方法
		
		HashMap<String, Object> AllInfo = new HashMap<>();
		
		HashMap<String, Object> hashsum = new HashMap<>(); 
		hashsum.put("partitionCount", KKConnector.partitionCount);
		hashsum.put("ReplicationFactor", KKConnector.ReplicationFactor);
		hashsum.put("config", KKConnector.config);
		AllInfo.put("sum",hashsum);
		
		HashMap<String, Object> hashdetail = new HashMap<>();
		hashdetail.put("Partition", KKConnector.partitionList);
		hashdetail.put("Leader", KKConnector.leaderList);
		hashdetail.put("ReplicasList", KKConnector.replicasList);
		hashdetail.put("Isr", KKConnector.AllIsrList);
		AllInfo.put("detail", hashdetail);
		
		
		root.put(tag, AllInfo);
		
		
		
	}

}
