package com.bytegen.common.kafka.monitor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * User: xiang
 * Date: 2018/8/7
 * Desc: Monitor zk actions.
 */
public class KafkaEventMonitor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaEventMonitor.class);

    /**
     * All action listeners, would receive the zk action events.
     */
    private final List<KafkaEventListener> subscribers;

    /**
     * Execute on action events
     */
    private final ExecutorService monitorExecutor;

    private KafkaEventMonitor() {
        subscribers = new ArrayList<>();
        subscribers.add(new LoggingEventListener());

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("kafka-monitor-subscribe-thread-%d")
                .setUncaughtExceptionHandler(
                        (t, e) -> logger.error(String.format("Zookeeper action monitor, thread[%s] throw : ", t.getName()), e))
                .build();
        monitorExecutor = Executors.newSingleThreadExecutor(namedThreadFactory);
    }

    public List<KafkaEventListener> getSubscribers() {
        return subscribers;
    }

    class LoggingEventListener implements KafkaEventListener {

        @Override
        public void onAction(String traceId, KafkaEventType type, String topicName,
                             String message, Object value) {
            logger.info("{\"trace_id\":\"" + traceId + "\",\"type\":\"" + type + "\",\"topic\":\"" + topicName
                    + "\",\"message\":\"" + message + "\",\"value\":\"" + value + "\"}");
        }
    }

    // singleton
    private static KafkaEventMonitor instance = new KafkaEventMonitor();

    public static KafkaEventMonitor getInstance() {
        return instance;
    }


    ///////////////////////
    // subscribe methods //

    public boolean addSubscriber(KafkaEventListener listener) {
        if (listener == null) {
            return false;
        }
        return subscribers.add(listener);
    }

    public boolean removeSubscriber(KafkaEventListener listener) {
        if (listener == null) {
            return false;
        }
        return subscribers.remove(listener);
    }

    public void triggerAction(String traceId, KafkaEventType type, String topicName,
                              String message, Object value) {
        monitorExecutor.execute(() -> {
            for (KafkaEventListener listener : subscribers) {
                listener.onAction(traceId, type, topicName, message, value);
            }
        });
    }
}
