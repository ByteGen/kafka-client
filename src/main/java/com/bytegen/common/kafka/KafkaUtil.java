package com.bytegen.common.kafka;

import com.bytegen.common.kafka.consumer.ConsumerSetting;
import com.bytegen.common.kafka.producer.ProducerSetting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public class KafkaUtil {
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtil.class);

    public static Properties getConsumerProperties(KafkaTopic topic, ConsumerSetting config) {
        Properties property = new Properties();
        property.put("bootstrap.servers", topic.getBootstrapServers());
        property.put("group.id", config.getGroupId());
        if (null != config.getOffsetReset())
            property.put("auto.offset.reset", config.getOffsetReset().name().toLowerCase());
        if (config.getAutoCommit()) {
            property.put("enable.auto.commit", "true");
            property.put("auto.commit.interval.ms", config.getAutoCommitIntervalMs());
        } else {
            property.put("enable.auto.commit", "false");
        }
        if (config.getSessionTimeoutMs() > 0)
            property.put("session.timeout.ms", config.getSessionTimeoutMs());
        if (config.getMaxPollRecords() > 0)
            property.put("max.poll.records", config.getMaxPollRecords());
        if (StringUtils.isNotBlank(topic.getKeyDeserializer()))
            property.put("key.deserializer", topic.getKeyDeserializer());
        if (StringUtils.isNotBlank(topic.getValueDeserializer()))
            property.put("value.deserializer", topic.getValueDeserializer());
        if (StringUtils.isNotBlank(topic.getSaslMechanism()))
            property.put("sasl.mechanism", topic.getSaslMechanism());
        if (StringUtils.isNotBlank(topic.getSecurityProtocol()))
            property.put("security.protocol", topic.getSecurityProtocol());

        if (StringUtils.isNotBlank(config.getSasl())) {
            property.put("sasl.jaas.config", config.getSasl());
        }
        logger.info("Kafka consumer properties: " + property);
        return property;
    }

    public static Properties getProducerProperties(KafkaTopic topic, ProducerSetting config) {
        Properties property = new Properties();
        property.put("bootstrap.servers", topic.getBootstrapServers());
        property.put("group.id", config.getGroupId());

        if (StringUtils.isNotBlank(topic.getKeySerializer()))
            property.put("key.serializer", topic.getKeySerializer());
        if (StringUtils.isNotBlank(topic.getValueSerializer()))
            property.put("value.serializer", topic.getValueSerializer());
        if (StringUtils.isNotBlank(topic.getSaslMechanism()))
            property.put("sasl.mechanism", topic.getSaslMechanism());
        if (StringUtils.isNotBlank(topic.getSecurityProtocol()))
            property.put("security.protocol", topic.getSecurityProtocol());

        if (StringUtils.isNotBlank(config.getSasl())) {
            // sasl.mechanism = PLAIN
            property.put("sasl.jaas.config", config.getSasl());
        }
        logger.info("Kafka producer properties: " + property);
        return property;
    }


    //////////////////////////////////
    ///      internal methods      ///
    //////////////////////////////////

    public static void shutdownSafely(ExecutorService executor, String description) {
        if (executor != null && !executor.isShutdown()) {
            logger.info("Trying to shutdown {}.", description);
            executor.shutdown();
        }

        while (executor != null && !executor.isTerminated()) {
            try {
                logger.info("Waiting for task : {} end: desc for {} ms, isShutdown: {}, isTerminated: {}, executor: {}.",
                        description, KafkaConstant.AWAIT_TERMINATION_MS, executor.isShutdown(),
                        executor.isTerminated(), executor);

                executor.awaitTermination(KafkaConstant.AWAIT_TERMINATION_MS, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.error("Waiting for active task termination fail.", e);
            }
        }
        logger.info("Shutdown task : {} successful.", description);
    }

    private static AtomicLong lastId = new AtomicLong(); // Auto increased id
    private static final String ip = resolveLocalIp(); // Local ip address
    private static final String SEPARATOR = "_";
    private static final int ID_CYCLE = 1000000;

    public static String generateTraceId() {
        // Rule： hexIp(ip)base36(timestamp)-seq
        final long startTimeStamp = System.currentTimeMillis();
        String traceId = hexIp(ip) + Long.toString(startTimeStamp, Character.MAX_RADIX) + SEPARATOR
                + lastId.incrementAndGet() % ID_CYCLE;
        MDC.put(KafkaConstant.REQUEST_ID, traceId);
        return traceId;
    }

    // ip address to hex string: 255.255.255.255 -> FFFFFFFF
    private static String hexIp(String ip) {
        final StringBuilder sb = new StringBuilder();
        for (String seg : ip.split("\\.")) {
            String h = Integer.toHexString(Integer.parseInt(seg));
            if (h.length() == 1) sb.append("0");
            sb.append(h);
        }
        return sb.toString();
    }

    /**
     * may be more than one ip is found
     */
    private static Set<InetAddress> resolveLocalAddresses() {
        Set<InetAddress> addrs = new HashSet<>();
        Enumeration<NetworkInterface> ns = null;
        try {
            ns = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException ignored) {
        }
        while (ns != null && ns.hasMoreElements()) {
            Enumeration<InetAddress> is = ns.nextElement().getInetAddresses();
            while (is.hasMoreElements()) {
                InetAddress i = is.nextElement();
                if (!i.isLoopbackAddress() && !i.isLinkLocalAddress() && !i.isMulticastAddress()
                        && !isSpecialIp(i.getHostAddress())) addrs.add(i);
            }
        }
        return addrs;
    }

    private static boolean isSpecialIp(String ip) {
        if (ip.contains(":")) return true;
        if (ip.startsWith("127.")) return true;
        if (ip.equals("255.255.255.255")) return true;
        return false;
    }

    private static String resolveLocalIp() {
        Set<InetAddress> addrs = resolveLocalAddresses();
        for (InetAddress addr : addrs) {
            return addr.getHostAddress();
        }
        return "";
    }
}
