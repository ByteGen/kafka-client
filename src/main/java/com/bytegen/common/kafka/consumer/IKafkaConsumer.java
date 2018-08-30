package com.bytegen.common.kafka.consumer;

import com.bytegen.common.kafka.IKafkaResource;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public interface IKafkaConsumer extends IKafkaResource {

    boolean consume() throws Exception;
}
