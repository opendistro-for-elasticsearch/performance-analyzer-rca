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
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaProducerController {
  public static final String NAME = "DecisionToKafkaPlugin";
  private static KafkaProducerController instance;
  private static final Logger LOG = LogManager.getLogger(KafkaProducerController.class);
  private PluginConfig pluginConfig = null;
  private KafkaProducer<String, String> kafkaProducerInstance = null;


  private KafkaProducerController() {
    String pluginConfPath = Paths.get(ConfConsts.CONFIG_DIR_PATH, ConfConsts.PLUGINS_CONF_FILENAMES).toString();
    pluginConfig = new PluginConfig(pluginConfPath);
    String bootstrapServer = pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_BOOTSTRAP_SERVER_KEY);
    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProducerInstance = new KafkaProducer<>(configProperties);
  }

  public static KafkaProducerController getInstance() {
    if (instance == null) {
      instance = new KafkaProducerController();
    }
    return instance;
  }

  public PluginConfig getSingletonPluginConfig() {
    return pluginConfig;
  }


  public KafkaProducer<String, String> getSingletonKafkaProducer() {
    return kafkaProducerInstance;
  }
}
