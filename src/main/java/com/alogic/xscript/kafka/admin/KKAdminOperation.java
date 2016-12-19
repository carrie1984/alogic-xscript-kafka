package com.alogic.xscript.kafka.admin;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alogic.xscript.AbstractLogiclet;
import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.anysoft.util.BaseException;
import com.anysoft.util.Properties;
import com.anysoft.util.PropertiesConstants;
import com.alogic.xscript.kafka.util.KKConnector;

/**
 * 管理命令虚基类
 * @author cuijialing
 *
 */
public abstract class KKAdminOperation extends AbstractLogiclet{
public KKAdminOperation(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}

protected String pid = "$mq-admin";
	
	/**
	 * 返回结果的id
	 */
	protected String id;
	
	
	public void configure(Properties p){
		super.configure(p);
		pid = PropertiesConstants.getString(p,"pid", pid,true);
		id = PropertiesConstants.getString(p,"id", "$" + getXmlTag(),true);
	}

	@Override
	protected void onExecute(Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher) {
		KKConnector conn = ctx.getObject(pid);
		if (conn == null){
			throw new BaseException("core.no_adminconn","It must be in a admin-conn context,check your script.");
		}
		
		if (StringUtils.isNotEmpty(id)){
			onExecute(conn,root,current,ctx,watcher);
		}
	}

	protected abstract void onExecute(KKConnector row, Map<String, Object> root,
			Map<String, Object> current, LogicletContext ctx,
			ExecuteWatcher watcher);

}
