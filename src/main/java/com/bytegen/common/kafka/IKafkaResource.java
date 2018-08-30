package com.bytegen.common.kafka;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public interface IKafkaResource {

    KafkaTopic getKafkaTopic();

    boolean isRunning();

    boolean isSecure();

    void close();
}
