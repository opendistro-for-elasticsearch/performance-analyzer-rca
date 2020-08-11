/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.opendestro.kafkaAdapter.starter;

import com.opendestro.kafkaAdapter.configuration.KafkaAdapterConf;
import com.opendestro.kafkaAdapter.configuration.ProducerConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.opendestro.kafkaAdapter.util.Helper;
import com.opendestro.kafkaAdapter.util.Target;
import org.apache.kafka.common.KafkaException;

import java.nio.file.Paths;
import java.util.Timer;
import java.util.TimerTask;


public class ProducerStarter {
    private static ObjectMapper mapper = new ObjectMapper();
    public static void writeToKafkaQueue(Target target, ProducerConfiguration producerConfig){
        Timer timer = new Timer();
        KafkaProducer<String, JsonNode> producer = producerConfig.createProducer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try{
                    String resp = Helper.makeRequest(target);
                    JsonNode jsonNode = mapper.readTree(resp);
                    ProducerRecord<String, JsonNode> record = new ProducerRecord<String, JsonNode>(producerConfig.getTopic(), jsonNode);
                    producer.send(record);
                    producer.flush();
                } catch (JsonProcessingException e){
                    System.out.println("Exception Found On Processing Json: " + e.getMessage());
                } catch (KafkaException e){
                    System.out.println("Exception Found on Kafka: " + e.getMessage());
                    producer.close();
                }
            }
        }, 0, producerConfig.getInterval());
    }

    public static void startProducer() {
        String kafkaAdapterConfPath = Paths.get(KafkaAdapterConsts.CONFIG_DIR_PATH, KafkaAdapterConsts.KAFKA_ADAPTER_FILENAME).toString();
        KafkaAdapterConf conf = new KafkaAdapterConf(kafkaAdapterConfPath);
        String bootstrapServer = conf.getKafkaBootstrapServer();
        String topic = conf.getKafkaTopicName();
        long interval = conf.getSendPeriodicityMillis();
        String params = conf.getQueryParams();
        String url = KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT + (params.equals("all") ? "" : "?name=" + params);
        Target target = new Target(url);
        ProducerConfiguration producerConfig = new ProducerConfiguration(bootstrapServer, topic, interval);
        writeToKafkaQueue(target, producerConfig);
    }
}
