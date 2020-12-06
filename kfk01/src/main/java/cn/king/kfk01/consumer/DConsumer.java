package cn.king.kfk01.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author: wjl@king.cn
 * @time: 2020/12/1 9:32
 * @version: 1.0.0
 * @description: 自定义存储offset. 示例代码.
 */
public class DConsumer {

    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        // 创建消费者配置信息
        Properties properties = new Properties();
        // 连接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 消费者组. 使用kfk控制台时会给我们自动分配, 但是代码中需要我们手动指定. 只要group.id相同, 就是与同一个消费者组.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kfk_consumer_group1");
        // 开启手动提交offset.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // k v 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建一个消费者. 泛型是读取到数据的k/v类型.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅主题 没有返回值. 订阅一个不存在的主题也是可以的, 但是会有一个警告.
        // TODO 注意此处第二个参数. 我们知道, 无论是谁保存offset, offset的值会在消费者启动的时候读取.
        consumer.subscribe(Collections.singletonList("first"), new ConsumerRebalanceListener() {
            // TODO 该方法会在Rebalance之前调用.
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 提交当前的offset
                commitOffset(currentOffset);
            }

            // TODO 该方法会在Rebalance之后调用.
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    // 定位到最近提交的offset位置继续消费
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });

        // 死循环拉取数据.
        while (true) {
            // 获取数据. 参数拉取的延迟时间.
            // 返回值有s, 说明一次能拉取多个值. 即批量获取.
            ConsumerRecords<String, String> records = consumer.poll(100);
            // 解析并打印ConsumerRecords
            records.forEach(record -> {
                System.out.println(record.key() + "--" + record.value());
                // TODO
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            });
            // TODO 异步提交
            commitOffset(currentOffset);
        }

    }

    // TODO 提交该消费者所有分区的offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
        // 可以将offset保存到mysql
    }

    // TODO 获取某分区的最新offset
    private static long getOffset(TopicPartition topicPartition) {
        // 可以从mysql获取offset
        return 0;
    }

}
