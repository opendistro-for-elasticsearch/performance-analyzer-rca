package com.harold.starter;

import com.harold.configuration.ProducerConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.harold.tool.Helper;
import com.harold.tool.Target;

import java.io.*;
import java.net.*;
import java.util.Objects;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;


public class ProducerStarter {

    private static ObjectMapper mapper = new ObjectMapper();
    public static void writeToKafkaQueue(Target target, ProducerConfiguration producerConfig, int count){
        Timer timer = new Timer();
        KafkaProducer<String, JsonNode> producer = producerConfig.CreateProducer();
        timer.scheduleAtFixedRate(new TimerTask() {
            int counter = 0;
            @Override
            public void run() {
                try{
                    String resp = Helper.makeRequest(target);
                    JsonNode jsonNode = mapper.readTree(resp);
//                    System.out.println("counter: "+counter+", data: "+jsonNode);
                    ProducerRecord<String, JsonNode> record = new ProducerRecord<String, JsonNode>(producerConfig.getTopic(), jsonNode);
                    producer.send(record);
                    counter ++;
                    if(counter == count){
                        System.out.println("finished sending response");
                        producer.close();
                        this.cancel();

                    }
                } catch (JsonProcessingException e){
                    e.printStackTrace();
                }
            }
        }, 0, producerConfig.getInterval());
    }

    public static void startProducer(Properties props) {
        String bootstrapServer = props.getProperty("bootstrap_server");
        String topic = props.getProperty("topic");
        int interval = Integer.parseInt(props.getProperty("kafka_producer_interval"));
        String params = props.getProperty("params");

        String url = "localhost:9600/_opendistro/_performanceanalyzer/rca" + (params.equals("all") ? "" : "?name=" + params);
        Target target = new Target(url);
        ProducerConfiguration producerConfig = new ProducerConfiguration( bootstrapServer, topic, interval);
        writeToKafkaQueue(target, producerConfig, 2); // write to Kafka Queue every 5 seconds
    }
}
