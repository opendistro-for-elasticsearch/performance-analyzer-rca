package com.harold.configuration;

import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class ProducerConfiguration {
    private String bootstrap_server;
    private String topic;
    private int interval;


    public ProducerConfiguration(String bootstrap_server, String topic, int interval){
        this.bootstrap_server = bootstrap_server;
        this.topic = topic;
        this.setInterval(interval);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        if(interval <= 0 || interval > 20000){
            this.interval = 5000;
        }else{
            this.interval = interval;
        }
    }

    public String getBootstrap_server() {
        return bootstrap_server;
    }

    public void setBootstrap_server(String bootstrap_server) {
        this.bootstrap_server = bootstrap_server;
    }

    public KafkaProducer<String, JsonNode> CreateProducer(){
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrap_server);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");
        return new KafkaProducer<>(configProperties);
    }
}

