mq-puller
============

mq-puller用于创建一个Consumer连接。

### 实现类

com.alogic.xscript.kafka.KKPuller

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | cid | connector的上下文对象id，缺省值为$mq-puller |
| 2 | bootstrapServer | bootstrapServer的连接地址：[ip:port]，缺省值为${server}，多个地址以“;”分隔 ,与生产者的设置对应|
| 3 | groupId | 消费者组名 |
| 4 | enableAutoCommit |是否自动提交已拉取消息的offset。提交offset即视为该消息已经成功被消费，该组下的Consumer无法再拉取到该消息（除非手动修改offset）。默认为true |
| 5 | autoCommitIntervalMs | 自动提交offset的间隔毫秒数，默认为5000 |
| 6 | keySerializer | 消息key的序列器Class，根据key和value的类型决定 |
| 7 | valueSerializer | 消息value的序列器Class，根据key和value的类型决定 |


### 案例
可以在conf/setting中设置参数${bootstrapServer}来指定bootstrapServer连接地址
```xml

	<?xml version="1.0"?>
	<settings>
	
		<parameter id="bootstrapServer" value="127.0.0.1:9092"/>
		
	</settings>

```

也可以在脚本中指定各个参数的值

```xml

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