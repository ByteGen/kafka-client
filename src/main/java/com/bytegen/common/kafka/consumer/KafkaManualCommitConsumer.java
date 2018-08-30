package com.bytegen.common.kafka.consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.bytegen.common.kafka.KafkaConstant;
import com.bytegen.common.kafka.KafkaTopic;
import com.bytegen.common.kafka.KafkaUtil;
import com.bytegen.common.kafka.monitor.KafkaEventMonitor;
import com.bytegen.common.kafka.monitor.KafkaEventType;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public class KafkaManualCommitConsumer implements IKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaManualCommitConsumer.class);

    private final Map<KafkaConsumer<byte[], byte[]>, Boolean> consumerMap;
    private volatile boolean runningStatus = true;

    private final ExecutorService consumerFetchEs;

    private final KafkaTopic kafkaTopic;
    private final ConsumerSetting consumerSetting;
    private final IConsumerWorker consumerWorker;

    public KafkaManualCommitConsumer(KafkaTopic consumerTopic, IConsumerWorker consumerWorker) {
        this(consumerTopic, null, consumerWorker);
    }

    public KafkaManualCommitConsumer(KafkaTopic consumerTopic, ConsumerSetting consumerSetting, IConsumerWorker consumerWorker) {
        Validate.notNull(consumerTopic, "Kafka topic cannot be null");
        Validate.notNull(consumerWorker, "Kafka consumer worker cannot be null");

        this.kafkaTopic = consumerTopic;
        this.consumerSetting = null == consumerSetting ? new ConsumerSetting() : consumerSetting;
        this.consumerWorker = consumerWorker;

        if (this.consumerSetting.getnThreads() <= 0) {
            this.consumerSetting.setnThreads(KafkaConstant.DEFAULT_EXECUTOR_COUNT);
        }
        this.consumerMap = new ConcurrentHashMap<>();
        this.consumerFetchEs = Executors.newFixedThreadPool(
                this.kafkaTopic.getPartitions(),
                new ThreadFactoryBuilder()
                        .setNameFormat(this.kafkaTopic.getName() + "-consumer-%d")
                        .setUncaughtExceptionHandler((t, e) ->
                                logger.error(String.format("Kafka manual commit consumer fetch thread[%s] throws: ", t.getName()), e))
                        .build()
        );
    }

    public void setRunningStatus(boolean runningStatus) {
        this.runningStatus = runningStatus;
    }

    public IConsumerWorker getConsumerWorker() {
        return consumerWorker;
    }

    /**
     * fetch message from broker
     */
    private void fetchBrokerMessage() {
        logger.info("Manual commit consumer startup, topic : {}", getKafkaTopic().getName());
        try {
            String fetchMonitorMessage = String.format("[%s]-consumer-sum", getKafkaTopic().getName());

            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(KafkaUtil.getConsumerProperties(kafkaTopic, consumerSetting));
            consumer.subscribe(Collections.singletonList(getKafkaTopic().getName()));
            consumerMap.put(consumer, true);

            while (runningStatus) {
                String traceId = KafkaUtil.generateTraceId();
                ConsumerRecords<byte[], byte[]> records = null;
                try {
                    records = consumer.poll(Long.MAX_VALUE);
                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (runningStatus) {
                        logger.error("WakeupException happened, but consumerStatus is true", e);
                    }
                } catch (Exception e) {
                    logger.error("Exception happened when parse message ", e);
                }

                try {
                    if (records != null) {
                        KafkaEventMonitor.getInstance().triggerAction(traceId, KafkaEventType.MESSAGE_FETCH,
                                getKafkaTopic().getName(), fetchMonitorMessage, records.count());
                        handleConsumerRecord(consumer, records);
                    }
                } catch (Exception e) {
                    logger.error("Exception happened when handle message ", e);
                }
            }

            consumer.close();
            consumerMap.put(consumer, false);
            logger.info("Close consumer: Topic [{}], consumer [{}] successfully", getKafkaTopic().getName(), consumer.toString());
        } catch (Exception e) {
            logger.error("Exception happened when consumer fetchBrokerMessage", e);
        }
    }

    protected void handleConsumerRecord(KafkaConsumer consumer, ConsumerRecords<byte[], byte[]> records) {

        String consumeMonitorMessage = String.format("[%s]-consume-latency", getKafkaTopic().getName());

        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                String requestId = KafkaUtil.generateTraceId();
                logger.info("Received record from kafka broker, partition: {}, offset: {}.",
                        record.partition(), record.offset());

                try {
                    byte[] msgValue = record.value();
                    long startTs = System.currentTimeMillis();
                    getConsumerWorker().consume(msgValue);

                    long latency = System.currentTimeMillis() - startTs;
                    KafkaEventMonitor.getInstance().triggerAction(requestId, KafkaEventType.MESSAGE_CONSUME,
                            getKafkaTopic().getName(), consumeMonitorMessage, latency);

                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(record.offset() + 1)));
                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (isRunning()) {
                        logger.error("WakeupException happened, but consumerRunning is true", e);
                    }
                } catch (Exception e) {
                    logger.error("Exception happened when handle message record, partition : {}, offset: {}",
                            record.partition(), record.offset(), e);
                }
            }
        }
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
        // All consumer should be closed
        boolean flag = true;
        for (Map.Entry<KafkaConsumer<byte[], byte[]>, Boolean> entry : consumerMap.entrySet()) {
            flag = flag & (!entry.getValue());
        }
        logger.info("{} kafka resource security state: {}", getKafkaTopic().getName(), flag);
        return flag;
    }

    @Override
    public void close() {
        if (isRunning()) {
            setRunningStatus(false);
            consumerMap.forEach((consumer, running) -> consumer.wakeup());
        }
        KafkaUtil.shutdownSafely(consumerFetchEs, "kafka-manual-commit-consumer[" + getKafkaTopic().getName() + "]");
    }

    @Override
    public boolean consume() throws Exception {
        for (int i = 0; i < getKafkaTopic().getPartitions(); i++) {
            consumerFetchEs.execute(this::fetchBrokerMessage);
        }
        return true;
    }
}
