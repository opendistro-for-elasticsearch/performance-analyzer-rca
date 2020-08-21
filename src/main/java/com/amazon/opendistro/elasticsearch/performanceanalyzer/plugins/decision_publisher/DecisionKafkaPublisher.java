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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.decision_publisher;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.KafkaProducerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.ConfConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;


public class DecisionKafkaPublisher extends Plugin implements ActionListener {

  public static final String NAME = "DecisionToKafkaPlugin";
  private static final Logger LOG = LogManager.getLogger(DecisionKafkaPublisher.class);
  private static HttpURLConnection httpConnection;
  private static PluginConfig pluginConfig = null;
  private static KafkaProducer<String, String> kafkaProducerInstance = null;
  private static KafkaProducerController controller;

  public void initialize() {
    if (controller == null) {
      controller = KafkaProducerController.getInstance();
    }
    pluginConfig = controller.getSingletonPluginConfig();
    kafkaProducerInstance = controller.getSingletonKafkaProducer();
  }

  public void sendDecisionSummaryToKafkaQueue(String msg) {
    try {
      String kafkaTopic = pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.DECISION_KAFKA_TOPIC_KEY);
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, msg);
      LOG.info(String.format("sending record: %s to kafka topic: %s ", msg, kafkaTopic));
      kafkaProducerInstance.send(record);
    } catch (KafkaException e) {
      LOG.error("Exception Found on Kafka: " + e.getMessage());
    }
  }

  public PluginConfig getPluginConfig() {
    return pluginConfig;
  }

  public KafkaProducer<String, String> getKafkaProducer() {
    return kafkaProducerInstance;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void actionPublished(Action action) {
    initialize();
    LOG.info("Action: [{}] published by decision maker publisher.", action.name());
    String summary = action.summary();
    sendDecisionSummaryToKafkaQueue(summary);
  }

  //For testing
  public void setKafkaProducerController(KafkaProducerController testController) {
    controller = testController;
  }

}
