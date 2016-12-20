mq-sender
============

mq-sender用于创建一个Producer连接。

### 实现类

com.alogic.xscript.kafka.KKSender

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | cid | connector的上下文对象id，缺省值为$mq-sender |
| 2 | bootstrapServers | Kafka集群连接串，可以由多个host:port组成：[ip:port]，缺省值为$bootstrapServers，多个地址以“;”分隔 |
| 3 | acks | broker消息确认的模式，有三种：0：不进行消息接收确认，即Client端发送完成后不会等待Broker的确认 .1：由Leader确认，Leader接收到消息后会立即返回确认信息.all：集群完整确认，Leader会等待所有in-sync的follower节点都确认收到消息后，再返回确认信息 ,默认为1|
| 4 | retries | 发送失败时Producer端的重试次数，默认为0 |
| 5 | batchSize | 当同时有大量消息要向同一个分区发送时，Producer端会将消息打包后进行批量发送。如果设置为0，则每条消息都独立发送。默认为16384字节 |
| 6 | lingerMs | 发送消息前等待的毫秒数，与batch.size配合使用，默认为0 |
| 7 | bufferMemory | 消息缓冲池大小。尚未被发送的消息会保存在Producer的内存中，如果消息产生的速度大于消息发送的速度，那么缓冲池满后发送消息的请求会被阻塞,默认33554432字节|
| 8 | keySerializer | 消息key的序列器Class，根据key和value的类型决定 |
| 9 | valueSerializer | 消息value的序列器Class，根据key和value的类型决定 |


### 案例
可以在conf/setting中设置参数${bootstrapServers}来指定Kafka集群连接串地址
```xml

	<?xml version="1.0"?>
	<settings>
	
		<parameter id="bootstrapServers" value="127.0.0.1:9092"/>
		
	</settings>

```

也可以在脚本中指定bootstrapServers和其他各个参数的值

```xml
		
		<?xml version="1.0"?>
<script>
<using xmlTag="mq-kafka" module="com.alogic.xscript.kafka.MQKafka"/>

<mq-kafka>
<mq-sender bootstrapServers="127.0.0.1:9092" 
			acks="all"
			retries="0"
			batchSize="16384"
			lingerMs="1"
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