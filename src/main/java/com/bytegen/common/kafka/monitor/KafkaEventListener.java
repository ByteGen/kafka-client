package com.bytegen.common.kafka.monitor;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc:
 */
public interface KafkaEventListener {
    void onAction(String traceId, KafkaEventType type, String topicName,
                  String message, Object value);
}
