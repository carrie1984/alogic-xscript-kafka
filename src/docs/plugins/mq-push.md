mq-push
=======

mq-push用于接收消息。

### 实现类

com.alogic.xscript.kafka.KKPush

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | pid | 父节点的元素名称，默认为$mq-pusher |
| 2 | id | 当前元素的id |
| 3 | topic | push模式接收消息的主题 |
| 4 | thread | 接收消息的线程数目  |
| 5 | waittime | 本次接收消息的等待时间 |

### 案例

```

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
	<mq-push topic="mykafka" thread="1" id="msg" waittime="2000">
				<!-- 调用子指令，对消息进行处理 -->
				<get id="${msg}" value="${msg}"></get>
	</mq-push>
	
</mq-pusher>
</mq-kafka>
</script>

```