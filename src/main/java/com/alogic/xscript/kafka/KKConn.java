//package com.alogic.xscript.kafka;
///*
// * 创建一个kafka连接，实际上kafka不用连接，本类主要是为了注册插件。
// */
//
//import java.util.Map;
//
//import org.apache.zookeeper.WatchedEvent;
//import org.apache.zookeeper.Watcher;
//
//import com.alogic.xscript.ExecuteWatcher;
//import com.alogic.xscript.Logiclet;
//import com.alogic.xscript.LogicletContext;
//import com.alogic.xscript.plugins.Segment;
//import com.anysoft.util.Properties;
//import com.anysoft.util.PropertiesConstants;
//
//
//public class KKConn extends Segment implements Watcher {
//	protected String cid = "$admin-conn";
//	protected String connectString = "${kafka.connectString}";
//
//	public KKConn(String tag, Logiclet p) {
//		super(tag, p);
//		// TODO Auto-generated constructor stub
//		registerModule("list-topic",KKListTopic.class);
//	}
//	@Override
//	public void configure(Properties p){
//		super.configure(p);
//		
//		cid = PropertiesConstants.getString(p,"cid",cid,true);
//		connectString = PropertiesConstants.getString(p,"connectString",connectString,true);
//	}
//	
//	@Override
//	protected void onExecute(Map<String, Object> root,
//			Map<String, Object> current, LogicletContext ctx, ExecuteWatcher watcher) {
//		ZooKeeperConnector conn = new ZooKeeperConnector(ctx,this,connectString);
//		try {
//			ctx.setObject(cid, conn);
//			super.onExecute(root, current, ctx, watcher);
//		}finally{
//			ctx.removeObject(cid);
//		}
//	}
//
//	public void process(WatchedEvent event) {
//
//	}	
//
//}
