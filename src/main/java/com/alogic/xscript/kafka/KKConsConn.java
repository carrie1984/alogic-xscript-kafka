package com.alogic.xscript.kafka;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.ConsumerConnector;
import com.alogic.xscript.plugins.Segment;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class KKConsConn extends Segment{

	public KKConsConn(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
		registerModule("receive", KKReceive.class);
	}
	protected String cid = "$consumer-conn";
	/*
	 * consumer 连接参数
	 */
	//Kafka集群连接串，可以由多个host:port组成
	protected String zookeeperConnector = "$zookeeperConnector";
	protected String groupId = "";
	protected int syncTimeMs = 200;
	protected int sessionTimeoutMs = 4000;
	protected int autoCommitIntervalMs = 1000;
	protected String autoOffsetReset = "smallest";
	protected String serializerClass = "kafka.serializer.StringEncoder";
	
	
	@Override
	public void configure(Properties p){
		super.configure(p);
		
		cid = PropertiesConstants.getString(p,"cid",cid,true);
		zookeeperConnector = PropertiesConstants.getRaw(p, "zookeeperConnector", zookeeperConnector);
		groupId = PropertiesConstants.getRaw(p, "groupId", groupId);
		syncTimeMs = PropertiesConstants.getInt(p, "syncTimeMs", syncTimeMs);
		sessionTimeoutMs = PropertiesConstants.getInt(p, "sessionTimeoutMs", sessionTimeoutMs);
		autoCommitIntervalMs = PropertiesConstants.getInt(p, "autoCommitIntervalMs", autoCommitIntervalMs);
		autoOffsetReset = PropertiesConstants.getRaw(p, "autoOffsetReset", autoOffsetReset);
		serializerClass = PropertiesConstants.getRaw(p, "valueSerializer", serializerClass);
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx, ExecuteWatcher watcher) 
	{
		System.out.println("==========consumer================");
		ConsumerConnector conn = new ConsumerConnector(ctx, zookeeperConnector, groupId, syncTimeMs, sessionTimeoutMs,autoCommitIntervalMs, autoOffsetReset, serializerClass);
		try {
			ctx.setObject(cid, conn);
			super.onExecute(root, current, ctx, watcher);
		}finally{
			ctx.removeObject(cid);
			//conn.disconnect();
		}
	}

}
