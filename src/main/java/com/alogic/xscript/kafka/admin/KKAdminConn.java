package com.alogic.xscript.kafka.admin;

import java.util.Map;

import com.alogic.xscript.kafka.util.KKConnector;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.plugins.Segment;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;

/*
 * kafka管理者
 * @author cuijialing
 */

public class KKAdminConn extends Segment{
	
	protected String cid = "$mq-admin";
	protected String zookeeperConnect = "${zookeeperConnect}";
	protected int sessionTimeoutMs;
	protected int connectionTimeoutMs;


	public KKAdminConn(String tag, Logiclet p) {
		super(tag, p);
		registerModule("create-topic", KKCreateTopic.class);
		registerModule("alter-topic", KKAlterTopic.class);
		registerModule("del-topic", KKDeleteTopic.class);
		registerModule("list-topic", KKListTopic.class);
		registerModule("desc-topic", KKDescribeTopic.class);
	}
	@Override
	public void configure(Properties p)
	{
		super.configure(p);
		cid = PropertiesConstants.getString(p, "cid",cid,true);
		zookeeperConnect = PropertiesConstants.getString(p, "zookeeperConnect", zookeeperConnect,true);
		sessionTimeoutMs = PropertiesConstants.getInt(p, "sessionTimeoutMs",sessionTimeoutMs);
		connectionTimeoutMs = PropertiesConstants.getInt(p, "connectionTimeoutMs", connectionTimeoutMs);
		
	}
	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx, ExecuteWatcher watcher)
	{
		KKConnector connector = new KKConnector(ctx, zookeeperConnect,sessionTimeoutMs, connectionTimeoutMs);
	//	System.out.println(connector.toString());
		try
		{
			ctx.setObject(cid, connector);
			super.onExecute(root, current, ctx, watcher);
		}finally
		{
			ctx.removeObject(cid);
			connector.disconnect();
		}
				
	}

}
