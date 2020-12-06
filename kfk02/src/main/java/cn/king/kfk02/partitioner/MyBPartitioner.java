package cn.king.kfk02.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author: wjl@king.cn
 * @time: 2020/12/6 10:33
 * @version: 1.0.0
 * @description: 自定义分区器.
 * 需求：将所有数据存储到topic的第0号分区上.
 * 新API.
 */
public class MyBPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 控制分区
        return 0;
    }

    @Override
    public void close() {

    }

}
