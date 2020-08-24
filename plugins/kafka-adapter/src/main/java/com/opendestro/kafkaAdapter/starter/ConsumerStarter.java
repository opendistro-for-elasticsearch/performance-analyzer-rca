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

package com.opendestro.kafkaAdapter.starter;

import com.opendestro.kafkaAdapter.configuration.ConsumerConfiguration;
import com.opendestro.kafkaAdapter.configuration.KafkaAdapterConf;
import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import com.opendestro.kafkaAdapter.util.Helper;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;


public class ConsumerStarter {
    private static final Logger LOG = LogManager.getLogger(ConsumerStarter.class);
    public static void runConsumer(ConsumerConfiguration consumerConfig, int maxNoFound, String webhooksUrl) {
        int noMessageFound = 0;
        KafkaConsumer<String, String> consumer = consumerConfig.createConsumer();
        consumer.subscribe(Collections.singletonList(consumerConfig.getTopic()));
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(consumerConfig.getInterval()));
                if (consumerRecords.count() == 0) {
                    noMessageFound++;
                    if (noMessageFound > maxNoFound) {
                        LOG.info("No response, terminating");
                        break;
                    } else {
                        continue;
                    }
                }
                consumerRecords.forEach(record -> {
                    Helper.postToSlackWebHook(record.value(), webhooksUrl);
                });
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            // ignore shutdown
        } finally {
            consumer.close();
            LOG.info("Shutting down the Kafka consumer");
        }
    }

    public static void startConsumer() {
        String kafkaAdapterConfPath = Paths.get(KafkaAdapterConsts.CONFIG_DIR_PATH, KafkaAdapterConsts.KAFKA_ADAPTER_FILENAME).toString();
        KafkaAdapterConf conf = new KafkaAdapterConf(kafkaAdapterConfPath);
        String bootstrapServer = conf.getKafkaBootstrapServer();
        String topic = conf.getKafkaTopicName();
        String webhooksUrl = conf.getWebhooksUrl();
        long interval = conf.getReceivePeriodicityMillis();
        int maxNoFound = conf.getMaxNoMessageFoundCountOnConsumer();
        ConsumerConfiguration consumerConfig = new ConsumerConfiguration(bootstrapServer, topic, interval);
        runConsumer(consumerConfig, maxNoFound, webhooksUrl);
    }
}
