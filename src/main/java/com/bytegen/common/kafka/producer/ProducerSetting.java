package com.bytegen.common.kafka.producer;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public class ProducerSetting {

    private String groupId;

    private String sasl;

    private int maxThreads;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getSasl() {
        return sasl;
    }

    public void setSasl(String sasl) {
        this.sasl = sasl;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    @Override
    public String toString() {
        return "{\"KafkaTopic1Producer\":"
                + super.toString()
                + ", \"groupId\":\"" + groupId + "\""
                + ", \"sasl\":\"" + sasl + "\""
                + ", \"maxThreads\":\"" + maxThreads + "\""
                + "}";
    }
}
