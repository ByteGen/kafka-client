package com.bytegen.common.kafka.consumer;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
// produce: ... --> producer worker --> producer --> kafka
// consume: kafka --> consumer --> consumer worker --> ...
public interface IConsumerWorker {

    /**
     * Convert kafka message to Object<T> for consumer
     */
    void consume(final byte[] msg);
}
