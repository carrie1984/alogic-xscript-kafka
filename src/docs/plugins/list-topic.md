list-topic
===========

list-topic用于获取主题列表。

### 实现类

com.alogic.xscript.kafka.admin.KKListTopic

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | tag | 元素返回数据的标签 |

### 案例

```xml

	<?xml version="1.0"?>
<script>
<using xmlTag="mq-kafka" module="com.alogic.xscript.kafka.MQKafka" />
<mq-kafka>
<mq-admin zookeeperConnect="127.0.0.1:2181" sessionTimeoutMs="10000" connectionTimeoutMs="8000">	
	<!-- 列出topic -->
	<list-topic tag="original">
	</list-topic>
	
</mq-admin>
</mq-kafka>
</script>

```