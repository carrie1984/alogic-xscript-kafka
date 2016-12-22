package com.alogic.xscript.kafka;

import com.alogic.xscript.Logiclet;
import com.alogic.xscript.plugins.Segment;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.LogicletContext;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;
import com.alogic.xscript.kafka.util.ConsuPullConn;

/**
 * 消费者的类型为pull，则采用主动方式进行消息消费，应用主动调用Consumer
 * 的拉取消息方法从Broker拉消息，主动权由应用控制 
 */
public class KKPuller extends Segment{
	protected String cid = "$mq-puller";
	/*
	 * kafka pull consumer 连接参数
	 */
	
	protected String bootstrapServer = "localhost:9092";
	protected String groupId = "test";
	protected String enableAutoCommit = "true";
	protected String autoCommitIntervalMs = "1000";
	protected String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
	protected String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
	
	

	public KKPuller(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
		registerModule("mq-pull", KKPull.class);
	}
	@Override
	public void configure(Properties p){
		super.configure(p);
		
		cid = PropertiesConstants.getString(p,"cid",cid,true);
		bootstrapServer = PropertiesConstants.getRaw(p, "bootstrapServer", bootstrapServer);
		groupId = PropertiesConstants.getRaw(p, "groupId", groupId);
		enableAutoCommit = PropertiesConstants.getString(p, "enableAutoCommit", enableAutoCommit);
		autoCommitIntervalMs = PropertiesConstants.getString(p, "autoCommitIntervalMs", autoCommitIntervalMs);
		keyDeserializer = PropertiesConstants.getRaw(p, "keyDeserializer", keyDeserializer);
		valueDeserializer = PropertiesConstants.getRaw(p, "valueSerializer", valueDeserializer);
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx, ExecuteWatcher watcher) 
	{
		
		ConsuPullConn conn = new ConsuPullConn(ctx, bootstrapServer, groupId, enableAutoCommit, autoCommitIntervalMs, keyDeserializer, valueDeserializer);
		try {
			ctx.setObject(cid, conn);
			super.onExecute(root, current, ctx, watcher);
		}finally{
			ctx.removeObject(cid);
			conn.disconnect();
		}
	}

}
