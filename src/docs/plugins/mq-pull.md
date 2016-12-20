mq-pull
=======

mq-pull用于接收消息。

### 实现类

com.alogic.xscript.kafka.KKPull

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | pid | 父节点的元素名称，默认为$mq-puller |
| 2 | id | 当前元素的id |
| 3 | topic | 接收消息的主题 |
| 4 | pollTimeMs | 拉取超时毫秒数  |


### 案例

```

	<?xml version="1.0"?>
<script>
<using xmlTag="mq-kafka" module="com.alogic.xscript.kafka.MQKafka"/>
<mq-kafka>
<mq-puller bootstrapServer="localhost:9092" 
			   groupId="test" 
			   enableAutoCommit="true" 
			   autoCommitIntervalMs="1000" 
			   keyDeserializer="org.apache.kafka.common.serialization.StringDeserializer"
			   valueDeserializer="org.apache.kafka.common.serialization.StringDeserializer"
			   > 
	<mq-pull id="msg" topic="mykafka" pollTimeMs="100">
		<!-- 调用子指令，对消息进行处理 -->
				<get id="${msg}" value="${msg}"></get>		
	</mq-pull>
</mq-puller>
</mq-kafka>
</script>

```