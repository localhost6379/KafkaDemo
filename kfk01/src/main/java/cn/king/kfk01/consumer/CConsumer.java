package cn.king.kfk01.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @author: wjl@king.cn
 * @time: 2020/11/16 0:41
 * @version: 1.0.0
 * @description: 手动提交offset. 异步提交.
 * 同步提交有失败重试机制, 因此更加可靠, 但是由于她会阻塞线程, 直到提交成功, 因此吞吐量会受到很大影响. 因此更多的情况下会选用异步提交offset的方式.
 */
public class CConsumer {

    public static void main(String[] args) {

        // 创建消费者配置信息
        Properties properties = new Properties();
        // 连接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // TODO 开启手动提交offset.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // k v 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费者组. 使用kfk控制台时会给我们自动分配, 但是代码中需要我们手动指定
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kfk_consumer_group1");

        // 创建一个消费者. 泛型是读取到数据的k/v类型.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅主题 没有返回值. 订阅一个不存在的主题也是可以的, 但是会有一个警告.
        consumer.subscribe(Arrays.asList("first", "second"));

        // 死循环拉取数据.
        while (true) {
            // 获取数据. 参数拉取的延迟时间.
            // 返回值有s, 说明一次能拉取多个值. 即批量获取.
            ConsumerRecords<String, String> records = consumer.poll(100);
            // 解析并打印ConsumerRecords
            records.forEach(record -> {
                System.out.println(record.key() + "--" + record.value());
            });

            // TODO 手动提交offset. 异步提交. 代码走到此处时开启新的线程提交offset. 异步方式效率高.
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (null != exception) System.err.println("手动提交offset失败, offset: " + offsets);
                }
            });

        }


        // 生产者启动后就不要关闭了
        // 关闭连接
        //consumer.close();
    }

}
