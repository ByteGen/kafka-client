package com.bytegen.common.kafka.consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.bytegen.common.kafka.KafkaConstant;
import com.bytegen.common.kafka.KafkaTopic;
import com.bytegen.common.kafka.KafkaUtil;
import com.bytegen.common.kafka.monitor.KafkaEventMonitor;
import com.bytegen.common.kafka.monitor.KafkaEventType;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public class KafkaAutoCommitConsumer implements IKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaAutoCommitConsumer.class);

    private final Map<KafkaConsumer<byte[], byte[]>, Boolean> consumerMap;
    private volatile boolean runningStatus = true;
    private final BlockingQueue<byte[]> localMessageQueue;

    private final ExecutorService consumerFetchEs;
    private final ExecutorService workerEs;

    private final KafkaTopic kafkaTopic;
    private final ConsumerSetting consumerSetting;
    private final IConsumerWorker consumerWorker;

    public KafkaAutoCommitConsumer(KafkaTopic consumerTopic, IConsumerWorker consumerWorker) {
        this(consumerTopic, null, consumerWorker);
    }

    public KafkaAutoCommitConsumer(KafkaTopic consumerTopic, ConsumerSetting consumerSetting, IConsumerWorker consumerWorker) {
        Validate.notNull(consumerTopic, "Kafka topic cannot be null");
        Validate.notNull(consumerWorker, "Kafka consumer worker cannot be null");

        this.kafkaTopic = consumerTopic;
        this.consumerSetting = null == consumerSetting ? new ConsumerSetting() : consumerSetting;
        this.consumerWorker = consumerWorker;

        // check setting value
        if (!this.consumerSetting.getAutoCommit()) {
            this.consumerSetting.setAutoCommit(true);
        }
        if (this.consumerSetting.getnThreads() <= 0) {
            this.consumerSetting.setnThreads(KafkaConstant.DEFAULT_EXECUTOR_COUNT);
        }
        this.consumerMap = new ConcurrentHashMap<>();
        this.consumerFetchEs = Executors.newFixedThreadPool(
                this.kafkaTopic.getPartitions(),
                new ThreadFactoryBuilder()
                        .setNameFormat(this.kafkaTopic.getName() + "-consumer-%d")
                        .setUncaughtExceptionHandler((t, e) ->
                                logger.error(String.format("Kafka auto commit consumer fetch thread[%s] throws: ", t.getName()), e))
                        .build()
        );

        this.localMessageQueue = new LinkedBlockingQueue<>();
        this.workerEs = Executors.newFixedThreadPool(
                this.consumerSetting.getnThreads(),
                new ThreadFactoryBuilder()
                        .setNameFormat(this.kafkaTopic.getName() + "-consumer-%d")
                        .setUncaughtExceptionHandler((t, e) ->
                                logger.error(String.format("Kafka auto commit consumer work thread[%s] throws: ", t.getName()), e))
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
        logger.info("Auto commit consumer startup, topic : {}", getKafkaTopic().getName());
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
                        handleConsumerRecord(records);
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

    private void handleConsumerRecord(ConsumerRecords<byte[], byte[]> records) {
        records.forEach(record -> {
            logger.info("Received record from kafka broker, partition: {}, offset: {}.",
                    record.partition(), record.offset());
            try {
                byte[] msgValue = record.value();
                localMessageQueue.put(msgValue);
            } catch (Exception e) {
                logger.error("Exception happened when handle message ", e);
            }
        });
    }

    private void consumeMessage() {
        logger.info("Kafka auto commit consumer processor startup, topic : {}", getKafkaTopic().getName());

        String localMessageQueuedMessage = String.format("[%s]-queue-size", getKafkaTopic().getName());
        String consumeMonitorMessage = String.format("[%s]-consume-latency", getKafkaTopic().getName());

        while (isRunning() || !localMessageQueue.isEmpty()) {
            try {
                String requestId = KafkaUtil.generateTraceId();
                // using poll() to replace with take() method
                byte[] message = localMessageQueue.poll(KafkaConstant.AWAIT_FOR_QUEUE_MS, TimeUnit.MILLISECONDS);
                if (message != null) {
                    logger.info("Taking message from queue, current queue size: {}", localMessageQueue.size());

                    long startTs = System.currentTimeMillis();
                    // do consume
                    getConsumerWorker().consume(message);
                    long latency = System.currentTimeMillis() - startTs;

                    KafkaEventMonitor.getInstance().triggerAction(requestId, KafkaEventType.QUEUED_SIZE,
                            getKafkaTopic().getName(), localMessageQueuedMessage, localMessageQueue.size());
                    KafkaEventMonitor.getInstance().triggerAction(requestId, KafkaEventType.MESSAGE_CONSUME,
                            getKafkaTopic().getName(), consumeMonitorMessage, latency);
                }
            } catch (Exception e) {
                logger.error("Error happened in kafka auto commit consumer", e);
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
        // All consumer should be closed and queue is empty
        boolean flag = localMessageQueue.isEmpty();
        for (Map.Entry<KafkaConsumer<byte[], byte[]>, Boolean> entry : consumerMap.entrySet()) {
            flag = flag & !entry.getValue();

        }
        logger.info("{} kafka resource security state: {}", getKafkaTopic().getName(), flag);
        return flag;
    }

    @Override
    public void close() {
        if (runningStatus) {
            runningStatus = false;
            consumerMap.forEach((consumer, running) -> consumer.wakeup());
        }
        KafkaUtil.shutdownSafely(consumerFetchEs, "kafka-auto-commit-consumer-fetch[" + getKafkaTopic().getName() + "]");
        KafkaUtil.shutdownSafely(workerEs, "kafka-auto-commit-consumer-worker[" + getKafkaTopic().getName() + "]");
    }

    @Override
    public boolean consume() throws Exception {
        for (int i = 0; i < this.kafkaTopic.getPartitions(); i++) {
            consumerFetchEs.execute(this::fetchBrokerMessage);
        }
        for (int i = 0; i < this.consumerSetting.getnThreads(); i++) {
            workerEs.execute(this::consumeMessage);
        }
        return true;
    }
}
