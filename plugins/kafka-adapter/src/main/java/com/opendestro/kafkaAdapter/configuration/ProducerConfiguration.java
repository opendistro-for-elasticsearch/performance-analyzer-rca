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
