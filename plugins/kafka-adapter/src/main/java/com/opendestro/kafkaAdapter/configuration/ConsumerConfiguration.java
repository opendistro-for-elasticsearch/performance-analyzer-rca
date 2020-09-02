/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class ConsumerConfiguration {
    private String bootstrapServer;
    private String topic;
    private long interval;

    public ConsumerConfiguration(String bootstrapServer, String topic, long interval) {
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.setInterval(interval);
    }

    public long getInterval() {
        return interval;
    }

    private void setInterval(long interval) {
        this.interval = Math.max(KafkaAdapterConsts.KAFKA_MINIMAL_RECEIVE_PERIODICITY, interval);
    }

    public String getTopic() {
        return topic;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public KafkaConsumer<String, String> createConsumer() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        return new KafkaConsumer<>(configProperties);
    }
}
