package com.alogic.xscript.kafka.util;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.util.control.Exception.Finally;

/*@author cuijialing
 * test producer
 */
public class KafkaConnector extends Thread {
	
	@SuppressWarnings("deprecation")
	private final kafka.javaapi.producer.Producer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();
	@SuppressWarnings("deprecation")
	public KafkaConnector(String topic)
    {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "10.22.10.139:9092");
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }
    @SuppressWarnings("deprecation")
	public void run() {
        int messageNo = 1;
        while (true)
        {
            String messageStr = new String("Message_" + messageNo);
            System.out.println("Send:" + messageStr);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            messageNo++;
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
