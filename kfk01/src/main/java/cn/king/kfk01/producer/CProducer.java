package cn.king.kfk01.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author: wjl@king.cn
 * @time: 2020/11/15 22:50
 * @version: 1.0.0
 * @description: 发送数据指定分区
 */
public class CProducer {

    public static void main(String[] args) {
        // 创建kfk的生产者配置信息
        Properties properties = new Properties();
        // 指定连接的kfk集群, broker-list
        properties.put("bootstrap.servers", "localhost:9092");
        // key序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            // 第二个参数, 指定发送到0号分区. king是自定义的消息的key
            // TODO 即指定分区就发送到指定的分区. 不指定分区指定key就使用key的哈希值来决定发往哪个分区. 不指定分区不指定key就轮询, 轮询注意不是从第0个分区开始
            producer.send(new ProducerRecord<>("first", 0, "king", "message-" + i), new Callback() {
                // onCompletion完成的. metadata该消息的元数据信息.
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null == exception) {
                        System.out.println(metadata.partition() + "--" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }

        // 关闭连接
        producer.close();
    }

}
