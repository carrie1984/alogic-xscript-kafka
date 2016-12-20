del-topic
===========

del-topic用于在指定broker或集群删除主题。

### 实现类

com.alogic.xscript.kafka.admin.KKDeleteTopic

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | topic | 主题名称 |


### 案例

```xml

	<?xml version="1.0"?>
<script>
<using xmlTag="mq-kafka" module="com.alogic.xscript.kafka.MQKafka" />
<mq-kafka>
<mq-admin zookeeperConnect="127.0.0.1:2181" sessionTimeoutMs="10000" connectionTimeoutMs="8000">	
	
	<!-- 删除topic -->
	<del-topic topic="test1219">
	</del-topic>
	
	
</mq-admin>
</mq-kafka>
</script>

```