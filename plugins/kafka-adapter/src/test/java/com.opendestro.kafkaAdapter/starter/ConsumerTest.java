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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import org.junit.Assert;

import java.nio.file.Paths;

public class ConsumerTest {
    private ConsumerConfiguration consumerConfig;

    @Test
    public void consumerConfigTest() {
        String kafkaAdapterConfPath = Paths.get(KafkaAdapterConsts.CONFIG_DIR_TEST_PATH, KafkaAdapterConsts.KAFKA_ADAPTER_TEST_FILENAME).toString();
        KafkaAdapterConf conf = new KafkaAdapterConf(kafkaAdapterConfPath);
        consumerConfig = new ConsumerConfiguration(conf.getKafkaBootstrapServer(), conf.getKafkaTopicName(), 5000);
        KafkaConsumer<String, String> consumer1 = consumerConfig.createConsumer();
        Assert.assertNotNull(consumer1);
        Assert.assertEquals("test_rca", consumerConfig.getTopic());
        Assert.assertEquals(KafkaAdapterConsts.KAFKA_MINIMAL_RECEIVE_PERIODICITY, consumerConfig.getInterval());
        Assert.assertEquals(KafkaAdapterConsts.DEFAULT_BOOTSTRAP_SERVER, consumerConfig.getBootstrap_server());
    }
}
