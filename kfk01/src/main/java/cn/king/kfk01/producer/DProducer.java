package cn.king.kfk01.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author: wjl@king.cn
 * @time: 2020/11/15 22:50
 * @version: 1.0.0
 * @description: 使用自定义的分区器
 */
public class DProducer {

    public static void main(String[] args) {
        // 创建kfk的生产者配置信息
        Properties properties = new Properties();
        // 指定连接的kfk集群, broker-list
        properties.put("bootstrap.servers", "localhost:9092");
        // key序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // TODO 指定要使用的分区器 partitioner.class
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "cn.king.kfk01.partitioner.MyPartitioner");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            // 第二个参数, 指定发送到0号分区. king是自定义的消息的key
            // onCompletion完成的. metadata该消息的元数据信息.
            producer.send(new ProducerRecord<>("first", 0, "king", "message-" + i), (metadata, exception) -> {
                if (null == exception) {
                    System.out.println(metadata.partition() + "--" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        // 关闭连接
        producer.close();
    }

}
