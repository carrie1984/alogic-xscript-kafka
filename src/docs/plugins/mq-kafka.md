mq-kafka
============

mq-kafka用于创建一个命名空间。

### 实现类

com.alogic.xscript.kafka.MQKafka

### 配置参数

支持下列参数：

| 编号 | 代码 | 说明 |
| ---- | ---- | ---- |
| 1 | cid | connector的上下文对象id，缺省值为$mq-kafka |


### 案例
可以在脚本中作为框架使用。
```xml

	<?xml version="1.0"?>
	<script>
		<!-- 引用mq-kafka指令的实现类 -->
		<using xmlTag="mq-kafka" module="com.alogic.xscript.kafka.MQkafka" />
	
		<mq-kafka>
		</mq-kafka>
	</script>
	    
```