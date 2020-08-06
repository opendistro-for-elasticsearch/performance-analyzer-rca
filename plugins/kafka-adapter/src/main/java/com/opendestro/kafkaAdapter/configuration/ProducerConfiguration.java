package com.opendestro.kafkaAdapter.configuration;

import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class ProducerConfiguration {
    private String bootstrap_server;
    private String topic;
    private long interval;


    public ProducerConfiguration(String bootstrap_server, String topic, long interval){
        this.bootstrap_server = bootstrap_server;
        this.topic = topic;
        this.setInterval(interval);
    }

    public String getTopic() {
        return topic;
    }

    public long getInterval() {
        return interval;
    }

    private void setInterval(long interval) { ;
        this.interval = Math.max(KafkaAdapterConsts.KAFKA_MINIMAL_SEND_PERIODICITY, interval);
    }

    public String getBootstrap_server() {
        return bootstrap_server;
    }

    public KafkaProducer<String, JsonNode> CreateProducer(){
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrap_server);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");
        return new KafkaProducer<>(configProperties);
    }
}
