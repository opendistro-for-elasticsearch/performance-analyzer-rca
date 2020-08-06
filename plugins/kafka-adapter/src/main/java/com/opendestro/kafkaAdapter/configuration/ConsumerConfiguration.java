package com.opendestro.kafkaAdapter.configuration;

import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class ConsumerConfiguration {
    private String bootstrap_server;
    private String topic;
    private long interval;

    public ConsumerConfiguration(String bootstrap_server, String topic, long interval){
        this.bootstrap_server = bootstrap_server;
        this.topic = topic;
        this.setInterval(interval);
    }

    public long getInterval() {
        return interval;
    }

    private void setInterval(long interval) {
        this.interval = Math.max(KafkaAdapterConsts.KAFKA_MINIMAL_RECEIVE_PERIODICITY,interval);
    }

    public String getTopic() {
        return topic;
    }

    public String getBootstrap_server() {
        return bootstrap_server;
    }

    public KafkaConsumer<String, JsonNode> createConsumer(){
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrap_server);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_group");
        return new KafkaConsumer<>(configProperties);
    }
}
