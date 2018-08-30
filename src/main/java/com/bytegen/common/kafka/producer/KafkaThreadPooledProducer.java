package com.bytegen.common.kafka.producer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.bytegen.common.kafka.KafkaConstant;
import com.bytegen.common.kafka.KafkaTopic;
import com.bytegen.common.kafka.KafkaUtil;
import com.bytegen.common.kafka.monitor.KafkaEventMonitor;
import com.bytegen.common.kafka.monitor.KafkaEventType;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public class KafkaThreadPooledProducer implements IKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaThreadPooledProducer.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private volatile boolean runningStatus = true;

    private final ThreadPoolExecutor workerEs;

    private final KafkaTopic kafkaTopic;
    private final ProducerSetting producerSetting;
    // private final IProducerWorker producerWorker;

    public KafkaThreadPooledProducer(KafkaTopic producerTopic) {
        this(producerTopic, null);
    }

    public KafkaThreadPooledProducer(KafkaTopic producerTopic, ProducerSetting producerSetting/*, IProducerWorker<T> producerWorker*/) {
        Validate.notNull(producerTopic, "Kafka topic cannot be null");
        // Validate.notNull(producerWorker, "Kafka producer worker cannot be null");

        this.kafkaTopic = producerTopic;
        this.producerSetting = null == producerSetting ? new ProducerSetting() : producerSetting;
        // this.producerWorker = producerWorker;

        if (this.producerSetting.getMaxThreads() <= 0) {
            this.producerSetting.setMaxThreads(KafkaConstant.DEFAULT_EXECUTOR_COUNT);
        }
        this.producer = new KafkaProducer<>(KafkaUtil.getProducerProperties(producerTopic, this.producerSetting));

        this.workerEs = new ThreadPoolExecutor(1, this.producerSetting.getMaxThreads(),
                60L, TimeUnit.SECONDS, new SynchronousQueue<>(),
                new ThreadFactoryBuilder()
                        .setNameFormat(this.kafkaTopic.getName() + "-producer-%d")
                        .setUncaughtExceptionHandler((t, e) ->
                                logger.error(String.format("Kafka multi thread producer work thread[%s] throws: ", t.getName()), e))
                        .build());
    }

    public void setRunningStatus(boolean runningStatus) {
        this.runningStatus = runningStatus;
    }

    // public IProducerWorker<T> getProducerWorker() {
    //     return producerWorker;
    // }

    @SuppressWarnings("unchecked")
    private Future<RecordMetadata> handleProduce(ProducerRecord data, Callback callback) {
        String localMessageQueuedMessage = String.format("[%s]-produce-pool-size", getKafkaTopic().getName());
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
        KafkaEventMonitor.getInstance().triggerAction(requestId, KafkaEventType.QUEUED_SIZE,
                getKafkaTopic().getName(), localMessageQueuedMessage, workerEs.getActiveCount());
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
        // Queue is empty
        boolean flag = null == workerEs || workerEs.isTerminated();
        logger.info("{} kafka resource security state: {}", getKafkaTopic().getName(), flag);
        return flag;
    }

    @Override
    public void close() {
        if (runningStatus) {
            runningStatus = false;
        }
        KafkaUtil.shutdownSafely(workerEs, "kafka-multi-thread-producer[" + getKafkaTopic().getName() + "]");
        producer.flush();
        producer.close();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord record, @Nullable Callback callback) throws Exception {
        if (!isRunning()) {
            throw new IllegalStateException(String.format("Kafka producer is not running for topic [%s]", getKafkaTopic().getName()));
        }

        if (record == null) {
            return null;
        }

        logger.debug(String.format("Produce kafka message [%s] to topic [%s]", record, getKafkaTopic().getName()));
        return workerEs.submit(() -> {
            RecordMetadata metadata = null;
            try {
                Future<RecordMetadata> metadataFuture = handleProduce(record, callback);
                metadata = null != metadataFuture ? metadataFuture.get() : null;
            } catch (Exception e) {
                logger.error("Error happened in kafka multi thread producer: " + record, e);
                if (null != callback) {
                    metadata = new RecordMetadata(
                            new TopicPartition(getKafkaTopic().getName(), record.partition()),
                            -1, -1, RecordBatch.NO_TIMESTAMP,
                            Long.valueOf(-1L), -1, -1);
                    callback.onCompletion(metadata, e);
                }
            }
            return metadata;
        });
    }
}
