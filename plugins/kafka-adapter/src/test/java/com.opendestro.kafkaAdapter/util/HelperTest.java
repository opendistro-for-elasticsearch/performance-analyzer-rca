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

package com.opendestro.kafkaAdapter.util;

import com.opendestro.kafkaAdapter.configuration.ConsumerConfiguration;
import com.opendestro.kafkaAdapter.configuration.KafkaAdapterConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class HelperTest {

    private static String testBootstrapServer = "localhost:9092";
    private static String testTopic;
    private static long testInterval = 5000;
    private static KafkaProducer<String, String> kafkaProducer;
    private static KafkaConsumer<String, String> kafkaConsumer;
    private static ConsumerConfiguration config;

    @Before
    public void targetSetup() {
        String kafkaAdapterConfPath = Paths.get(KafkaAdapterConsts.CONFIG_DIR_TEST_PATH, KafkaAdapterConsts.KAFKA_ADAPTER_TEST_FILENAME).toString();
        KafkaAdapterConf conf = new KafkaAdapterConf(kafkaAdapterConfPath);
        testTopic = conf.getKafkaTopic();
        System.out.println("test topic: "+testTopic);
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBootstrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<>(producerProps);
    }

    @Test
    public void decisionPublisherTest() {
        String testMessage = "{\"test key\": \"test value\"}";
        config = new ConsumerConfiguration(testBootstrapServer, testTopic, testInterval);
        kafkaConsumer = config.createConsumer();
        kafkaConsumer.subscribe(Collections.singletonList(testTopic));
        ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, testMessage);
        kafkaProducer.send(record);
        kafkaProducer.close();
        ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
        kafkaConsumer.close();
        System.out.println("record count:" + records.count());
        Assert.assertEquals(1, records.count());
        Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
        ConsumerRecord<String, String> res = recordIterator.next();
        Assert.assertEquals("{\"text\":\"{\\\"test key\\\": \\\"test value\\\"}\"}", Helper.formatString(res.value()));
    }
}
