package com.alogic.xscript.kafka.admin;

import java.util.List;
import java.util.Map;

import com.alogic.xscript.ExecuteWatcher;
import com.alogic.xscript.Logiclet;
import com.alogic.xscript.LogicletContext;
import com.alogic.xscript.kafka.util.KKConnector;

public class KKListTopic extends KKAdminOperation{

	public KKListTopic(String tag, Logiclet p) {
		super(tag, p);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void onExecute(KKConnector row, Map<String, Object> root, Map<String, Object> current,
			LogicletContext ctx, ExecuteWatcher watcher) {
		// TODO Auto-generated method stub
		List<String> topiclist = KKConnector.ListTopic();
		//得到列表后需要将他转换为json格式便于处理，此时的topiclist已经是可以输出的结果。
		//需要将其转换为json格式更加方便
		
	}

}
