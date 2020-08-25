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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class HelperTest {

    private Target target1;
    private Target target2;
    private static String testBootstrapServer = "localhost:9092";
    private static String testTopic = "test_consumer";
    private static long testInterval = 5000;
    private static KafkaProducer<String, String> kafkaProducer;
    private static KafkaConsumer<String, String> kafkaConsumer;
    private static ConsumerConfiguration config;

    @Before
    public void targetSetup() {
        target1 = new Target(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT, "ClusterTemperatureRca");
        target2 = new Target(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT);
        Assert.assertEquals("http://localhost:9600/_opendistro/_performanceanalyzer/rca?name=ClusterTemperatureRca", target1.getUrl());
        Assert.assertEquals("http://localhost:9600/_opendistro/_performanceanalyzer/rca", target2.getUrl());
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBootstrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<>(producerProps);
    }

    @Test
    public void endpointTest() {
        Assert.assertEquals(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT, target1.getEndpoint());
        target1.setEndpoint("localhost:9650");
        Assert.assertEquals("localhost:9650", target1.getEndpoint());
    }

    @Test
    public void parameterTest() {
        Assert.assertNull(target2.getParameter());
        Assert.assertEquals("ClusterTemperatureRca", target1.getParameter());
        target2.setParameter("HighHeapUsageClusterRca");
        Assert.assertEquals("HighHeapUsageClusterRca", target2.getParameter());
    }

    //    @Test
    //    public void decisionPublisherTest() {
    //        String testMessage = "{\"test key\": \"test value\"}";
    //        config = new ConsumerConfiguration(testBootstrapServer, testTopic, testInterval);
    //        kafkaConsumer = config.createConsumer();
    //        kafkaConsumer.subscribe(Collections.singletonList(testTopic));
    //        ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, testMessage);
    //        kafkaProducer.send(record);
    //        kafkaProducer.close();
    //        ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
    //        kafkaConsumer.close();
    //        Assert.assertEquals(1, records.count());
    //        Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
    //        ConsumerRecord<String, String> res = recordIterator.next();
    //        Assert.assertEquals("{\"text\":\"{\\n  \\\"test key\\\" : \\\"test value\\\"\\n}\"}", Helper.formatString(res.value()));
    //    }
}