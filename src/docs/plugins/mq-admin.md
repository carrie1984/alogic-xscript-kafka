mq-admin
============

mq-admin用于创建一个Admin连接。

### 实现类

com.alogic.xscript.kafka.admin.KKAdminConn

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | cid | connector的上下文对象id，缺省值为$admin-conn |
| 2 | zookeeperConnect | zookeeperConnect的连接地址：[ip:port]，缺省值为${zookeeperConnect}，多个地址以“;”分隔 |
| 3 | sessionTimeoutMs | 会话超时时间 |
| 4 | connectionTimeoutMs | zookeeper连接超时时间 |


### 案例
可以在conf/setting中设置参数${zookeeperConnect}来指定zookeeper的连接地址。
```xml

	<?xml version="1.0"?>
	<settings>
	
		<parameter id="zookeeperConnect" value="127.0.0.1:2181"/>
		
	</settings>

```

也可以在脚本中指定各个参数的值。

```xml

	<?xml version="1.0"?>
<script>
<using xmlTag="mq-kafka" module="com.alogic.xscript.kafka.MQKafka" />
<mq-kafka>
<mq-admin zookeeperConnect="127.0.0.1:2181" sessionTimeoutMs="10000" connectionTimeoutMs="8000">	
	
</mq-admin>
</mq-kafka>
</script>
	    
```