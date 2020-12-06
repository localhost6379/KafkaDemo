package cn.king.kfk01.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author: wjl@king.cn
 * @time: 2020/11/15 22:50
 * @version: 1.0.0
 * @description: 带拦截器的生产者
 */
public class EProducer {

    public static void main(String[] args) throws Exception {
        // 创建kfk的生产者配置信息
        Properties properties = new Properties();
        // 指定连接的kfk集群, broker-list
        properties.put("bootstrap.servers", "localhost:9092");
        // key序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // TODO 添加拦截器 链. 执行顺序是add顺序.
        ArrayList<String> interceptorList = new ArrayList<>();
        interceptorList.add("cn.king.kfk01.interceptor.AInterceptor");
        interceptorList.add("cn.king.kfk01.interceptor.BInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorList);

        // 指定要使用的分区器 partitioner.class
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "cn.king.kfk01.partitioner.MyPartitioner");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            // 第二个参数, 指定发送到0号分区. king是自定义的消息的key
            producer.send(new ProducerRecord<>("first", 0, "king", "message-" + i));
        }

        // 关闭连接
        // todo 只有调用了下面的close()方法, 拦截器中的close()才会被调用. 可以将下面的close()方法放到finally中
        producer.close();
    }

}
