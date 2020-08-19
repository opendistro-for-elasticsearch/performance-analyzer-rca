/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.ConfConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;
import java.nio.file.Paths;
import java.util.Properties;

public class KafkaProducerController {


    public static final String NAME = "DecisionToKafkaPlugin";
    private static final Logger LOG = LogManager.getLogger(KafkaProducerController.class);
    private static PluginConfig pluginConfig = null;
    private static KafkaProducer<String, String> kafkaProducerInstance = null;


    public static PluginConfig getSingletonPluginConfig() {
        if (pluginConfig == null) {
            String pluginConfPath = Paths.get(ConfConsts.CONFIG_DIR_PATH, ConfConsts.PLUGINS_CONF_FILENAMES).toString();
            return new PluginConfig(pluginConfPath);
        }
        return pluginConfig;
    }


    public static KafkaProducer<String, String> getSingletonKafkaProducer(){
        if(kafkaProducerInstance == null){
            Properties configProperties = new Properties();
            configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_BOOTSTRAP_SERVER_KEY));
            configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducerInstance = new KafkaProducer<>(configProperties);
        }
        return kafkaProducerInstance;
    }
}
