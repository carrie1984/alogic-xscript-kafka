mq-send
=======

mq-send用于发送消息。

### 实现类

com.alogic.xscript.kafka.KKSend

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | pid | 父节点的元素名称，默认为$mq-sender |
| 2 | topic | 发送消息的主题 |
| 3 | key | 消息的key，同时也会作为partition的key  |
| 4 | value | 发送消息的内容 |

### 案例

```

	<?xml version="1.0"?>
<script>
<using xmlTag="mq-kafka" module="com.alogic.xscript.kafka.MQKafka"/>

<mq-kafka>
<mq-sender bootstrapServers="127.0.0.1:9092" 
			acks="all"
			retries="0"
			batchSize="16384"
			lingerMs="0"
			bufferMemory="33554432"
			keySerializer="org.apache.kafka.common.serialization.StringSerializer"
			valueSerializer="org.apache.kafka.common.serialization.StringSerializer"
			>
	<mq-send topic="mykafka" key="1" value="1713h1"></mq-send>
	<mq-send topic="mykafka" key="1" value="1713h2"></mq-send>
	<mq-send topic="mykafka" key="1" value="1713h3"></mq-send>
	<mq-send topic="mykafka" key="1" value="1713h4"></mq-send>
	
</mq-sender>
</mq-kafka>
</script>

```