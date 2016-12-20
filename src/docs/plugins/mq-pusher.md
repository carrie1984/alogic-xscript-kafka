mq-pusher
============

mq-pusher用于创建一个Consumer连接。

### 实现类

com.alogic.xscript.kafka.KKPusher

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | cid | connector的上下文对象id，缺省值为$mq-pusher |
| 2 | zookeeperConnector | Consumer的zookeeper连接串，要和broker的配置一致。形式为[ip:port]，缺省值为${zookeeperConnector}，多个地址以“;”分隔 |
| 3 | groupId | Consumer的组ID，相同goup.id的consumer属于同一个组。|
| 4 | sessionTimeoutMs | zookeeper的会话超时时间。 |
| 5 | syncTimeMs | zookeeper同步的时间，可以允许zookeeper follower 比 leader慢的时长 |
| 6 | autoCommitIntervalMs | Consumer提交offset值到zookeeper的周期。|
| 7 | autoOffsetReset | 当zookeeper中没有初始offset或者offset超出范围时对offset的操作，有smallest和largest |
| 8 | serializerClass | 序列化类参数 |


### 案例
可以在conf/setting中设置参数${server}来指定zookeeperConnector连接地址
```xml

	<?xml version="1.0"?>
	<settings>
	
		<parameter id="zookeeperConnector" value="127.0.0.1:2181"/>
		
	</settings>

```

也可以在脚本中指定各个参数的值

```xml

	<?xml version="1.0"?>
<script>
<using xmlTag="mq-kafka" module="com.alogic.xscript.kafka.MQKafka"/>
<mq-kafka>
<mq-pusher zookeeperConnector="127.0.0.1:2181" 
			   groupId="test" 
			   sessionTimeoutMs="4000" 
			   syncTimeMs="200"
			   autoCommitIntervalMs="1000" 
			   autoOffsetReset="smallest"
			   serializerClass="kafka.serializer.StringEncoder"
	
			   > 
			   <!-- 接收消息 -->
	<mq-push topic="mykafka" thread="1" id="msg">
				<!-- 调用子指令，对消息进行处理 -->
				<get id="${msg}" value="${msg}"></get>
			<mq-wait timeout="2000"/>
		
	</mq-push>
</mq-pusher>
</mq-kafka>
</script>
	
```