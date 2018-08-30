package com.bytegen.common.kafka;

import java.nio.charset.Charset;

/**
 * User: xiang
 * Date: 2018/8/8
 * Desc:
 */
public interface KafkaConstant {
    String REQUEST_ID = "request_id";
    String BOOTSTRAP_SERVERS = "bootstrap.servers";

    Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    String DEFAULT_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    int DEFAULT_AUTO_COMMIT_INTERVAL_MS = 1000;
    int DEFAULT_SESSION_TIMEOUT_MS = 6000;
    int DEFAULT_MAX_POLL_RECORDS = 500;
    int DEFAULT_EXECUTOR_COUNT = 3;

    long AWAIT_FOR_QUEUE_MS = 300;
    long AWAIT_TERMINATION_MS = 500;

    int DEFAULT_PARTITIONS = 1;
    int DEFAULT_REPLICATION_FACTOR = 3;
}
