package com.bytegen.common.kafka.monitor;

/**
 * User: xiang
 * Date: 2018/8/9
 * Desc:
 */
public enum KafkaEventType {
    CONSUMER_CREATE, CONSUMER_UPDATE, CONSUMER_DELETE,

    MESSAGE_PRODUCE, MESSAGE_FETCH, MESSAGE_CONSUME,

    QUEUED_SIZE,
}
