package com.bytegen.common.kafka.consumer;

import com.bytegen.common.kafka.KafkaConstant;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * User: xiang
 * Date: 2018/8/16
 * Desc:
 */
public class ConsumerSetting {

    private String groupId;
    private OffsetResetStrategy offsetReset;

    private String sasl;

    private boolean autoCommit = true;
    private int autoCommitIntervalMs = KafkaConstant.DEFAULT_AUTO_COMMIT_INTERVAL_MS;

    private int sessionTimeoutMs = KafkaConstant.DEFAULT_SESSION_TIMEOUT_MS;
    private int maxPollRecords = KafkaConstant.DEFAULT_MAX_POLL_RECORDS;

    private int nThreads = KafkaConstant.DEFAULT_EXECUTOR_COUNT;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public OffsetResetStrategy getOffsetReset() {
        return offsetReset;
    }

    public void setOffsetReset(OffsetResetStrategy offsetReset) {
        this.offsetReset = offsetReset;
    }

    public String getSasl() {
        return sasl;
    }

    public void setSasl(String sasl) {
        this.sasl = sasl;
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(int autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public int getnThreads() {
        return nThreads;
    }

    public void setnThreads(int nThreads) {
        this.nThreads = nThreads;
    }

    @Override
    public String toString() {
        return "{\"KafkaTopicConsumer\":"
                + super.toString()
                + ", \"groupId\":\"" + groupId + "\""
                + ", \"sasl\":\"" + sasl + "\""
                + ", \"autoCommit\":\"" + autoCommit + "\""
                + ", \"autoCommitIntervalMs\":\"" + autoCommitIntervalMs + "\""
                + ", \"sessionTimeoutMs\":\"" + sessionTimeoutMs + "\""
                + ", \"maxPollRecords\":\"" + maxPollRecords + "\""
                + ", \"nThreads\":\"" + nThreads + "\""
                + "}";
    }
}
