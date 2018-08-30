package com.bytegen.common.kafka;

import com.bytegen.common.kafka.consumer.ConsumerSetting;
import com.bytegen.common.kafka.consumer.KafkaManualCommitConsumer;
import com.bytegen.common.kafka.producer.KafkaSynchronizedProducer;
import com.bytegen.common.kafka.producer.ProducerSetting;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public class LoggingKafkaWorkerTest {
    private static final Logger logger = LoggerFactory.getLogger(LoggingKafkaWorkerTest.class);

    private LoggingConsumerWorker worker = new LoggingConsumerWorker();
    private KafkaTopic topic;
    private ConsumerSetting consumerSetting;
    private ProducerSetting producerSetting;

    @Before
    public void setUp() throws Exception {
        topic = new KafkaTopic();
        topic.setBootstrapServers("10.125.253.112:9092,10.125.253.68:9092,10.125.253.37:9092,10.125.252.83:9092,10.125.252.114:9092");
        topic.setName("swc-cvs-tsp-dev-80001-vb_sync_insert");
        topic.setPartitions(3);
        topic.setReplicationFactor(3);
        topic.setSaslMechanism("PLAIN");
        topic.setSecurityProtocol("SASL_PLAINTEXT");

        consumerSetting = new ConsumerSetting();
        consumerSetting.setGroupId("remote_vehicle_xxx");
        consumerSetting.setAutoCommit(true);
        consumerSetting.setMaxPollRecords(20);
        consumerSetting.setnThreads(2);
        consumerSetting.setSasl("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"SRFxjAzdFqD0\" password=\"Gmsw4wxDu5oT\";");

        producerSetting = new ProducerSetting();
        producerSetting.setGroupId("remote_vehicle_xxx");
        producerSetting.setMaxThreads(2);
        producerSetting.setSasl("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"SRFxjAzdFqD0\" password=\"Gmsw4wxDu5oT\";");

        logger.info("//////////////////////");
        logger.info("/////    init    /////");
        logger.info("//////////////////////");
    }

    @Test
    public void produce() throws Exception {
        KafkaSynchronizedProducer xx = new KafkaSynchronizedProducer(topic, producerSetting);
        xx.send(new ProducerRecord<>(topic.getName(), ("{\"funny_id\":" + System.currentTimeMillis() + "}").getBytes()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                logger.info("//////////////////////");
                logger.info("/////   sent     /////");
                logger.info("//////////////////////");
                logger.info(null == metadata ? null : metadata.toString());
            }
        });
    }


    @Test
    public void consume() throws Exception {
        KafkaManualCommitConsumer xx = new KafkaManualCommitConsumer(topic, consumerSetting, worker);
        xx.consume();

        Thread.sleep(10000);
    }



}