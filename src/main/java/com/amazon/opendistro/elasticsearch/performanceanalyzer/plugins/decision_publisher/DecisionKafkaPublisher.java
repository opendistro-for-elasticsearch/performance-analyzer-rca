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
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DecisionKafkaPublisher extends Plugin implements ActionListener {

  public static final String NAME = "DecisionToKafkaPlugin";
  private static final Logger LOG = LogManager.getLogger(DecisionKafkaPublisher.class);
  private KafkaProducer<String, String> kafkaProducerInstance;
  private KafkaProducerController controller;
  private String kafkaTopic;

  public DecisionKafkaPublisher(){
    controller = KafkaProducerController.getInstance();
    kafkaProducerInstance = controller.getSingletonKafkaProducer();
    kafkaTopic = controller.getSingletonPluginConfig().getKafkaDecisionListenerConfig(ConfConsts.DECISION_KAFKA_TOPIC_KEY);
  }

  public void sendDecisionSummaryToKafkaQueue(String msg) {
    try {
      ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, msg);
      LOG.debug(String.format("sending record: %s to kafka topic: %s ", msg, kafkaTopic));
      kafkaProducerInstance.send(record);
    } catch (KafkaException e) {
      LOG.error("Exception Found on Kafka: " + e.getMessage());
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void actionPublished(Action action) {
    LOG.debug("Action: [{}] published by decision maker publisher.", action.name());
    String summary = action.summary();
    sendDecisionSummaryToKafkaQueue(summary);
  }

  @VisibleForTesting
  public void setKafkaProducerController(KafkaProducerController testController) {
    controller = testController;
  }

  @VisibleForTesting
  public void setKafkaTopic(String topic) {
    this.kafkaTopic = topic;
  }

  @VisibleForTesting
  public void setKafkaProducerInstance(KafkaProducer<String, String> kafkaProducer) {
    kafkaProducerInstance = kafkaProducer;
  }
}
