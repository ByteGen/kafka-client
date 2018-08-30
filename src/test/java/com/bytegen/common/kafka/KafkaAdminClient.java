package com.bytegen.common.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by dandan.ao on 2018/8/17.
 */
public class KafkaAdminClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminClient.class);
    private AdminClient adminClient;
    private String bootstrapServers;

    public KafkaAdminClient(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        Properties props = new Properties();
        props.put(KafkaConstant.BOOTSTRAP_SERVERS, bootstrapServers);
        this.adminClient = AdminClient.create(props);
    }

    public KafkaAdminClient(Properties props) {
        this.bootstrapServers = props.getProperty(KafkaConstant.BOOTSTRAP_SERVERS);
        this.adminClient = AdminClient.create(props);
    }

    public void close() {
        if(this.adminClient != null) {
            this.adminClient.close();
        }
    }

    public KafkaTopic createTopic(String topicName) throws Exception {
        return this.createTopic(topicName, KafkaConstant.DEFAULT_PARTITIONS, KafkaConstant.DEFAULT_REPLICATION_FACTOR);
    }

    public KafkaTopic createTopic(String topicName, int partitions, int replicationFactor) throws Exception {
        NewTopic topic = new NewTopic(topicName, partitions, (short)replicationFactor);
        Collection<NewTopic> topics = Arrays.asList(topic);
        CreateTopicsResult result = adminClient.createTopics(topics);
        result.all().get();
        return new KafkaTopic(bootstrapServers, topicName, partitions, replicationFactor);
    }

    // use console command to delete
    public DeleteTopicsResult deleteTopic(String topicName) throws Exception {
        Collection<String> topics = Arrays.asList(topicName);
        DeleteTopicsResult result = adminClient.deleteTopics(topics);
        return result;
        //result.all().get();
        //return true;
    }

    public Set<String> listTopis() throws Exception {
        ListTopicsResult result = adminClient.listTopics();
        Set<String> topics = Collections.EMPTY_SET;
        topics = (Set)result.names().get();
        return topics;
    }

    public KafkaTopic getTopicDescribe(String topicName) throws Exception {
        Collection<String> topics = Arrays.asList(topicName);
        DescribeTopicsResult result = adminClient.describeTopics(topics);
        try {
            Map<String, TopicDescription> topicDescriptionMap = (Map)result.all().get(1000, TimeUnit.MILLISECONDS);
            if(topicDescriptionMap.containsKey(topicName)) {
                TopicDescription description = (TopicDescription)topicDescriptionMap.get(topicName);
                List<TopicPartitionInfo> partitions = description.partitions();
                int partitionNum = partitions.size();
                int replicaNum = partitions.get(0).replicas().size();
                return new KafkaTopic(bootstrapServers, description.name(), partitionNum, replicaNum);
            }
        } catch (Exception e) {
            if (e instanceof ExecutionException) {
                if (e.getMessage().contains("UnknownTopicOrPartitionException")) {
                    return null;
                }
            }
        }
        return null;
    }

    public Map<String, KafkaTopic> getTopicsDescribe(Collection<String> topics) throws Exception {
        Map<String, KafkaTopic> resMap = new HashMap<>();
        DescribeTopicsResult result = adminClient.describeTopics(topics);
        Map<String, TopicDescription> topicDescriptionMap = (Map)result.all().get();
        topics.forEach(topicName -> {
            if(topicDescriptionMap.containsKey(topicName)) {
                TopicDescription description = (TopicDescription)topicDescriptionMap.get(topicName);
                List<TopicPartitionInfo> partitions = description.partitions();
                int partitionNum = partitions.size();
                int replicaNum = partitions.get(0).replicas().size();
                resMap.put(topicName, new KafkaTopic(bootstrapServers, description.name(), partitionNum, replicaNum));
            }
        });
        return resMap;
    }

}
