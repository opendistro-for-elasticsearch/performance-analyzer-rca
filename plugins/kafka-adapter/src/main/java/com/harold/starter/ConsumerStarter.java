package com.harold.starter;

import com.harold.configuration.ConsumerConfiguration;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.harold.tool.Helper;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ConsumerStarter {
    public static void runConsumer(ConsumerConfiguration consumerConfig, int max_no_found, String webhook_url) {
        int noMessageFound = 0;
        int counter = 0;
        KafkaConsumer<String, JsonNode> consumer = consumerConfig.createConsumer();
        consumer.subscribe(Collections.singletonList(consumerConfig.getTopic()));
        try{
            while (true) {
                ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(Duration.ofMillis(consumerConfig.getInterval())); // setting 5 seconds as waiting interval
                if (consumerRecords.count() == 0) {
                    noMessageFound++;
                    if (noMessageFound > max_no_found) { // if no response lasting 5 times, the consumer will terminate
                        System.out.println("No response, terminating");
                        break;
                    } else {
                        continue;
                    }
                }
                //print each record.
                consumerRecords.forEach(record -> {
                    Helper.postToSlackWebHook(record.value(), webhook_url);
                    System.out.println("Record" + record.value());
                });
                // commits the offset of record to broker.
                consumer.commitAsync();
            }
        } catch (WakeupException e){
            // ignore shutdown
        } finally {
            consumer.close();
            System.out.println("Shutting down the consumer");
        }
    }

    public static void startConsumer(Properties props) {
        String bootstrapServer = props.getProperty("bootstrap_server");
        String topic = props.getProperty("topic");
        String webhook_url = props.getProperty("webhook_url");
        int interval = Integer.parseInt(props.getProperty("kafka_consumer_interval"));
        int max_no_found = Integer.parseInt(props.getProperty("max_no_message_found_count"));
        ConsumerConfiguration consumerConfig = new ConsumerConfiguration(bootstrapServer, topic, interval);
        runConsumer(consumerConfig, max_no_found, webhook_url);
    }
}
