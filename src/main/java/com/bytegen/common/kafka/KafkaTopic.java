package com.bytegen.common.kafka;

import org.apache.commons.lang3.StringUtils;

/**
 * User: xiang
 * Date: 2018/8/8
 * Desc:
 */
public class KafkaTopic {

    private String bootstrapServers;

    private String name;
    private Integer partitions;
    private Integer replicationFactor;

    private String saslMechanism;
    private String securityProtocol;

    private String keySerializer;
    private String valueSerializer;

    private String keyDeserializer;
    private String valueDeserializer;

    public KafkaTopic() {}

    public KafkaTopic(String name) {
        this.name = name;
    }

    public KafkaTopic(String bootstrapServers, String name, Integer partitions, Integer replicationFactor) {
        this.bootstrapServers = bootstrapServers;
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public Integer getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getKeySerializer() {
        if (StringUtils.isNotBlank(keySerializer)) {
            return keySerializer;
        } else {
            return KafkaConstant.DEFAULT_SERIALIZER;
        }
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getValueSerializer() {
        if (StringUtils.isNotBlank(valueSerializer)) {
            return valueSerializer;
        } else {
            return KafkaConstant.DEFAULT_SERIALIZER;
        }
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public String getKeyDeserializer() {
        if (StringUtils.isNotBlank(keyDeserializer)) {
            return keyDeserializer;
        } else {
            return KafkaConstant.DEFAULT_DESERIALIZER;
        }
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        if (StringUtils.isNotBlank(valueDeserializer)) {
            return valueDeserializer;
        } else {
            return KafkaConstant.DEFAULT_DESERIALIZER;
        }
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public String toString() {
        return "{\"KafkaTopic\":{"
                + "\"bootstrapServers\":\"" + bootstrapServers + "\""
                + ", \"name\":\"" + name + "\""
                + ", \"partitions\":\"" + partitions + "\""
                + ", \"replicationFactor\":\"" + replicationFactor + "\""
                + ", \"securityProtocol\":\"" + securityProtocol + "\""
                + ", \"keySerializer\":\"" + keySerializer + "\""
                + ", \"valueSerializer\":\"" + valueSerializer + "\""
                + ", \"keyDeserializer\":\"" + keyDeserializer + "\""
                + ", \"valueDeserializer\":\"" + valueDeserializer + "\""
                + "}}";
    }
}
