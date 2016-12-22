package com.alogic.xscript.kafka;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.ProducerConnector;
import com.alogic.xscript.plugins.Segment;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;


public class KKSender extends Segment {
	public KKSender(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
		registerModule("mq-send", KKSend.class);
	}
	protected String cid = "$mq-sender";
	/*
	 * producer的连接参数
	 */
	//Kafka集群连接串，可以由多个host:port组成
	protected String bootstrapServers = "$bootstrapServers";
	//broker消息确认的模式,默认为1
	protected String acks = "1";
	//发送失败时Producer端的重试次数，默认为0
	protected String retries = "0";
	protected String batchSize = "16384";
	protected String lingerMs = "1";
	protected String bufferMemory = "33554432";
	protected String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
	protected String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

	@Override
	public void configure(Properties p)
	{
		super.configure(p);
		cid = PropertiesConstants.getString(p,"cid",cid,true);
		bootstrapServers = PropertiesConstants.getRaw(p, "bootstrapServers", bootstrapServers);
		acks = PropertiesConstants.getRaw(p, "acks", acks);
		retries = PropertiesConstants.getString(p, "retries", retries);
		batchSize = PropertiesConstants.getString(p, "batchSize", batchSize);
		lingerMs = PropertiesConstants.getString(p, "lingerMs", lingerMs);
		bufferMemory = PropertiesConstants.getString(p, "bufferMemory", bufferMemory);
		keySerializer = PropertiesConstants.getRaw(p, "keySerializer", keySerializer);
		valueSerializer = PropertiesConstants.getRaw(p, "valueSerializer", valueSerializer);
	}
	
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx, ExecuteWatcher watcher) {
		ProducerConnector conn = new ProducerConnector(ctx,bootstrapServers,acks,retries,batchSize,lingerMs,bufferMemory,keySerializer,valueSerializer);
		try {
			ctx.setObject(cid, conn);
			super.onExecute(root, current, ctx, watcher);
		}finally{
			ctx.removeObject(cid);
			conn.disconnect();
		}
	}
}
