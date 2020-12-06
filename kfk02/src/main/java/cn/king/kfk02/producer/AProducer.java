package cn.king.kfk02.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author: wjl@king.cn
 * @time: 2020/12/6 10:28
 * @version: 1.0.0
 * @description: 创建生产者 -- 过时的API
 */
public class AProducer {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("request.required.acks", "1");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        Producer<Integer, String> producer = new Producer<>(new ProducerConfig(properties));

        KeyedMessage<Integer, String> message = new KeyedMessage<>("first", "hello world");
        producer.send(message );
    }

}
