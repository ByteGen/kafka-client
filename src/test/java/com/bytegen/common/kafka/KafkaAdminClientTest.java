package com.bytegen.common.kafka;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by dandan.ao on 2018/8/20.
 */
public class KafkaAdminClientTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminClientTest.class);

    private String bootstrapServers = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";

    @Test
    @Ignore
    public void testGetTopicDescribe() {
        KafkaAdminClient kafkaAdminClient = new KafkaAdminClient(bootstrapServers);
        String topic = "notification";
        try {
            KafkaTopic kafkaTopic = kafkaAdminClient.getTopicDescribe(topic);
            LOGGER.debug("topic desc:{}", kafkaTopic);

            Set<String> topics = kafkaAdminClient.listTopis();
            LOGGER.debug("topics:{}", topics);
        } catch (Exception e) {
            LOGGER.error("error, ", e);
        } finally {
            kafkaAdminClient.close();
        }
    }

    @Test
    @Ignore
    public void testCreateTopic() {
        KafkaAdminClient kafkaAdminClient = new KafkaAdminClient(bootstrapServers);
        String topic = "notification_non_99";
        try {
            KafkaTopic kafkaTopic;
            kafkaTopic = kafkaAdminClient.getTopicDescribe(topic);

            //kafkaTopic = kafkaAdminClient.createTopic(topic);
            LOGGER.debug("topic desc:{}", kafkaTopic);

            KafkaTopic newkafkaTopic = kafkaAdminClient.getTopicDescribe(topic);
            LOGGER.debug("actual topic desc:{}", newkafkaTopic);
        } catch (Exception e) {
            LOGGER.error("error, ", e);
        } finally {
            kafkaAdminClient.close();
        }
    }
}
