package com.bytegen.common.kafka;

import com.bytegen.common.kafka.consumer.IConsumerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: xiang
 * Date: 2018/8/8
 * Desc:
 */
public class LoggingConsumerWorker implements IConsumerWorker {
    private static final Logger logger = LoggerFactory.getLogger(LoggingConsumerWorker.class);

    @Override
    public void consume(byte[] msg) {
        if (null != msg) {
            logger.info("Consume kafka message: " + new String(msg));
        } else {
            logger.warn("Receive NULL kafka message!");
        }
    }

}
