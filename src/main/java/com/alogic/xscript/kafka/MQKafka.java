package com.alogic.xscript.kafka;

import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.admin.KKAdminConn;
import com.alogic.xscript.plugins.Segment;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

public class MQKafka extends Segment{
	protected String cid = "$mq-kafka";

	public MQKafka(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
		registerModule("mq-sender", KKSender.class);
		registerModule("mq-pusher", KKPusher.class);
		registerModule("mq-puller", KKPuller.class);
		registerModule("mq-admin", KKAdminConn.class);
		registerModule("send-msg", SendMsg.class);
		registerModule("kk-mq-sender", KKProducer.class);
	}
	
	@Override
	public void configure(Properties p)
	{
		super.configure(p);
		cid = PropertiesConstants.getString(p, "cid", cid);
	}
	
	@Override
	protected void onExecute(Map<String, Object> root, Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {

		super.onExecute(root, current, ctx, watcher);

	}


}
