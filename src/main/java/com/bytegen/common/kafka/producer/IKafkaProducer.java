package com.bytegen.common.kafka.producer;

import com.bytegen.common.kafka.IKafkaResource;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.annotation.Nullable;
import java.util.concurrent.Future;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public interface IKafkaProducer extends IKafkaResource {
    default Future<RecordMetadata> send(ProducerRecord record) throws Exception {
        return this.send(record, null);
    }

    Future<RecordMetadata> send(ProducerRecord record, @Nullable Callback callback) throws Exception;
}
