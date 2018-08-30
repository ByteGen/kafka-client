package com.bytegen.common.kafka.producer;

import com.bytegen.common.kafka.KafkaTopic;
import com.bytegen.common.kafka.KafkaUtil;
import com.bytegen.common.kafka.monitor.KafkaEventMonitor;
import com.bytegen.common.kafka.monitor.KafkaEventType;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public class KafkaSynchronizedProducer implements IKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSynchronizedProducer.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private volatile boolean runningStatus = true;

    private final KafkaTopic kafkaTopic;
    private final ProducerSetting producerSetting;
    // private final IProducerWorker<T> producerWorker;

    public KafkaSynchronizedProducer(KafkaTopic producerTopic) {
        this(producerTopic, null);
    }

    public KafkaSynchronizedProducer(KafkaTopic producerTopic, ProducerSetting setting/*, IProducerWorker<T> producerWorker*/) {
        Validate.notNull(producerTopic, "Kafka topic cannot be null");
        // Validate.notNull(producerWorker, "Kafka producer worker cannot be null");

        this.kafkaTopic = producerTopic;
        this.producerSetting = null == setting ? new ProducerSetting() : setting;
        // this.producerWorker = producerWorker;

        this.producer = new KafkaProducer<>(KafkaUtil.getProducerProperties(producerTopic, this.producerSetting));
    }

    public void setRunningStatus(boolean runningStatus) {
        this.runningStatus = runningStatus;
    }

    // public IProducerWorker<T> getProducerWorker() {
    //     return producerWorker;
    // }

    @SuppressWarnings("unchecked")
    private Future<RecordMetadata> handleProduce(ProducerRecord data, Callback callback) {
        String producerMonitorMessage = String.format("[%s]-produce-latency", getKafkaTopic().getName());
        String requestId = KafkaUtil.generateTraceId();

        long startTs = System.currentTimeMillis();
        // do produce
        // byte[] message = getProducerWorker().produce(data);
        // producer.send(new ProducerRecord<>(getKafkaTopic().getName(), message));
        Future<RecordMetadata> future = producer.send(data, callback);
        long latency = System.currentTimeMillis() - startTs;

        KafkaEventMonitor.getInstance().triggerAction(requestId, KafkaEventType.MESSAGE_PRODUCE,
                getKafkaTopic().getName(), producerMonitorMessage, latency);
        return future;
    }


    @Override
    public KafkaTopic getKafkaTopic() {
        return kafkaTopic;
    }

    @Override
    public boolean isRunning() {
        return runningStatus;
    }

    @Override
    public boolean isSecure() {
        return true;
    }

    @Override
    public void close() {
        if (runningStatus) {
            runningStatus = false;
        }
        producer.flush();
        producer.close();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord record, Callback callback) throws Exception {
        if (!isRunning()) {
            throw new IllegalStateException(String.format("Kafka producer is not running for topic [%s]", getKafkaTopic().getName()));
        }

        if (record == null) {
            return null;
        }

        logger.debug(String.format("Produce kafka message [%s] to topic [%s]", record, getKafkaTopic().getName()));
        return handleProduce(record, callback);
    }
}
