package com.alogic.xscript.kafka;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.ConsumerConnector;
import com.alogic.xscript.plugins.Segment;
import com.alogic.xscript.plugins.Sleep;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class KKPusher extends Segment{

	public KKPusher(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
		registerModule("mq-push", KKPush.class);
		registerModule("mq-wait", Sleep.class);
	}
	protected String cid = "$mq-pusher";
	/*
	 * consumer 连接参数
	 */
	//Kafka集群连接串，可以由多个host:port组成
	protected String zookeeperConnector = "$zookeeperConnector";
	protected String groupId = "";
	protected String syncTimeMs = "200";
	protected String sessionTimeoutMs = "4000";
	protected String autoCommitIntervalMs = "1000";
	protected String autoOffsetReset = "smallest";
	protected String serializerClass = "kafka.serializer.StringEncoder";
	
	
	@Override
	public void configure(Properties p){
		super.configure(p);
		
		cid = PropertiesConstants.getString(p,"cid",cid,true);
		zookeeperConnector = PropertiesConstants.getRaw(p, "zookeeperConnector", zookeeperConnector);
		groupId = PropertiesConstants.getRaw(p, "groupId", groupId);
		syncTimeMs = PropertiesConstants.getString(p, "syncTimeMs", syncTimeMs);
		sessionTimeoutMs = PropertiesConstants.getString(p, "sessionTimeoutMs", sessionTimeoutMs);
		autoCommitIntervalMs = PropertiesConstants.getString(p, "autoCommitIntervalMs", autoCommitIntervalMs);
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
			conn.disconnect();
		}
	}

}
