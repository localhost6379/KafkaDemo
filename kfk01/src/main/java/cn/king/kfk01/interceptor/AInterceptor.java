package cn.king.kfk01.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author: wjl@king.cn
 * @time: 2020/11/22 16:54
 * @version: 1.0.0
 * @description: 在发送的消息前面加上时间戳
 */
public class AInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 获取要发送的数据
        String value = record.value();
        // 创建一个新的ProducerRecord对象返回
        return new ProducerRecord<>(record.topic(), record.partition(), record.key(), System.currentTimeMillis() + "," + value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {
    }

}
