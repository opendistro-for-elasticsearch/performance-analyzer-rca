package com.harold.starter;

import com.harold.configuration.ConsumerConfiguration;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.harold.tool.Helper;
import java.time.Duration;
import java.util.Collections;


public class ConsumerStarter {
    static int MAX_NO_MESSAGE_FOUND_COUNT = 5;
    public static void runConsumer(ConsumerConfiguration consumerConfig) {
        int noMessageFound = 0;
        int counter = 0;
        KafkaConsumer<String, JsonNode> consumer = consumerConfig.createConsumer();
        consumer.subscribe(Collections.singletonList(consumerConfig.getTopic()));
        try{
            while (true) {
                ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(Duration.ofMillis(consumerConfig.getInterval())); // setting 5 seconds as waiting interval
                if (consumerRecords.count() == 0) {
                    noMessageFound++;
                    if (noMessageFound > MAX_NO_MESSAGE_FOUND_COUNT) { // if no response lasting 5 times, the consumer will terminate
                        System.out.println("No response, terminating");
                        break;
                    } else {
                        continue;
                    }
                }
                //print each record.
                consumerRecords.forEach(record -> {
                    Helper.postToSlackWebHook(record.value());
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

    public static void startConsumer() {
        String bootstrapServer = "localhost:9092";
        ConsumerConfiguration consumerConfig = new ConsumerConfiguration(bootstrapServer,"rca", 5000);
        runConsumer(consumerConfig);
    }
}
