package cn.king.kfk01.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author: wjl@king.cn
 * @time: 2020/11/22 16:54
 * @version: 1.0.0
 * @description: 消息发送成功后会更新发送成功的消息数 或 发送失败的消息数
 */
public class BInterceptor implements ProducerInterceptor<String, String> {

    private int success, error;

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 发送成功
        if (null != metadata) {
            success++;
            // 发送失败
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("发送成功条数: " + success);
        System.out.println("发送失败条数: " + error);
    }

}
