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

import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;

public class ConfTest {
    @Test
    public void testKafkaAdapterConfig() {
        String kafkaAdapterConfPath = Paths.get(KafkaAdapterConsts.CONFIG_DIR_TEST_PATH, KafkaAdapterConsts.KAFKA_ADAPTER_TEST_FILENAME).toString();
        KafkaAdapterConf conf = new KafkaAdapterConf(kafkaAdapterConfPath);
        Assert.assertEquals(kafkaAdapterConfPath, conf.getConfigFileLoc());
        Assert.assertEquals("localhost:9092", conf.getKafkaBootstrapServer());
        Assert.assertEquals("decision-rca-test", conf.getKafkaTopic());
        Assert.assertEquals(20000, conf.getReceivePeriodicityMillis());
        Assert.assertEquals(3, conf.getMaxNoMessageFoundCountOnConsumer());
    }
}
