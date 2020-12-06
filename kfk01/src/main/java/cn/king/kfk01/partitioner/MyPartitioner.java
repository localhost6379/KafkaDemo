package cn.king.kfk01.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author: wjl@king.cn
 * @time: 2020/11/16 0:04
 * @version: 1.0.0
 * @description: 自定义的分区器
 */
public class MyPartitioner implements Partitioner {

    /**
     * @author: wjl@king.cn
     * @createTime: 2020/11/16 0:05
     * @param: topic
     * @param: key
     * @param: keyBytes
     * @param: value
     * @param: valueBytes
     * @param: cluster
     * @return: int
     * @description: 参数传递的是Object和byte, 但是我们发送的消息可能是String, 说明消息进入分区器之前先进入了序列化器.
     * <p>
     * 我们在参数中能看到消息的key和value, 那么我们自定义分区器的时候能按照key分区, 也能按照value分区.
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 到1号分区
        //return 1;

        // 获取当前主题的分区数
        Integer integer = cluster.partitionCountForTopic(topic);
        // 使用key的哈希值与分区数进行取模来决定消息分配到哪个分区中.
        return key.toString().hashCode() % integer;

    }

    // 关闭
    @Override
    public void close() {
    }

    // 读配置信息
    @Override
    public void configure(Map<String, ?> configs) {
    }

}
