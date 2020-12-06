package cn.king.kfk01.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author: wjl@king.cn
 * @time: 2020/11/15 22:50
 * @version: 1.0.0
 * @description: 普通的生产者 发送数据"没有回调"
 */
public class AProducer {

    public static void main(String[] args) {
        // 创建kfk的生产者配置信息
        Properties properties = new Properties();
        // 指定连接的kfk集群, broker-list
        properties.put("bootstrap.servers", "localhost:9092");
        // 等待所有副本节点的应答. ack的应答级别.
        //properties.put("acks", "all");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数
        properties.put("retries", 3);
        // 批次大小. 一次发送多少大小的数据. 16k
        properties.put("batch.size", 16384);
        // 等待时间. 1ms. 两个参数控制发送数据, 要么数据大于16k,要么1ms之后发送数据.
        properties.put("linger.ms", 1);
        // 发送缓存区RecordAccumulator内存大小. 32M.
        // 数据到了16k发送到RecordAccumulator, RecordAccumulator 中最多放32M数据.
        properties.put("buffer.memory", 33554432);
        // key序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        // TODO public Future<RecordMetadata> send(ProducerRecord<K, V> record);
        // public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);
        // Callback中封装了发送的数据在kfk中的信息.
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "msg-" + i));
        }

        //Thread.sleep(10);

        // 关闭连接
        producer.close();

        /*
         * 实际上循环10次时间很快, 到不了1ms, 如果不写producer.close(), 那么数据是不会被发送到RecordAccumulator(数据发送缓冲区)中的, 消费者是不会消费到数据的.
         * 我们可以注释掉producer.close()并打开Thread.sleep(10);进行测试
         */

        /*
         * 上面的封装到properties中的所有参数, 在 ProducerConfig 类中都有. 只要记住这个类即可.
         * 实际上封装配置参数的常量类有3个:
         * ProducerConfig 封装生产者参数的常量类
         * ConsumerConfig 封装消费者参数的常量类
         * CommonClientConfigs 封装生产者和消费者共有的参数的常量类
         */

    }

}
