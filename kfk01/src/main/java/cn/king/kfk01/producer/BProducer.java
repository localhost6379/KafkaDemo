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
 * @description: 普通的生产者 发送数据"有回调"
 */
public class BProducer {

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
        // public Future<RecordMetadata> send(ProducerRecord<K, V> record);
        // TODO public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);
        // Callback中封装了发送的数据在kfk中的信息.
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "message-" + i), new Callback() {
                // onCompletion: 完成的. metadata: 该消息的元数据信息.
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null == exception) {
                        // 如果发送成功, 打印消息的分区编号和偏移量
                        System.out.println(metadata.partition() + "--" + metadata.offset());
                        /*
                            0--11
                            0--12
                            0--13
                            0--14
                            0--15
                            1--11
                            1--12
                            1--13
                            1--14
                            1--15
                         */
                    } else {
                        // 如果发送失败, 打印堆栈信息
                        exception.printStackTrace();
                    }
                }
            });
        }

        // 关闭连接
        producer.close();
    }

}
