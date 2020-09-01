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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.cluster_rca_publisher;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.KafkaProducerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.ConfConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummaryListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterSummaryKafkaPublisher extends Plugin implements ClusterSummaryListener {
    private static final Logger LOG = LogManager.getLogger(ClusterSummaryKafkaPublisher.class);
    private static final String NAME = "Kafka_Publisher_Plguin";
    private KafkaProducer<String, String> kafkaProducerInstance = null;
    private KafkaProducerController controller;
    private String kafkaTopic;

    public ClusterSummaryKafkaPublisher(){
        controller = KafkaProducerController.getInstance();
        kafkaProducerInstance = controller.getSingletonKafkaProducer();
        kafkaTopic = controller.getSingletonPluginConfig().getKafkaClusterRcaListenerConfig(ConfConsts.CLUSTER_SUMMARY_KAFKA_TOPIC_KEY);
    }

    public void sendRcaClusterSummaryToKafkaQueue(String msg) {
        try {
            msg = sanitizeMessage(msg);
            ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, msg);
            LOG.debug(String.format("sending record: %s to kafka topic: %s ", msg, kafkaTopic));
            kafkaProducerInstance.send(record);
        } catch (KafkaException e) {
            LOG.error("Exception Found on Kafka: " + e.getMessage());
        }
    }

    public JsonObject getJsonData(ClusterSummary clusterSummay) {
        LOG.debug("Generating rca cluster Json data");
        JsonObject jsonObject = new JsonObject();
        if (clusterSummay.summaryMapIsEmpty()) {
            LOG.debug("clusterSummary is empty");
        } else {
            clusterSummay.getSummaryMap().forEach((k, v) -> {
                jsonObject.add(k, v.toJson());
            });
        }
        return jsonObject;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void summaryPublished(ClusterSummary clusterSummary) {
        if (!clusterSummary.summaryMapIsEmpty()) {
            JsonObject jsonObject = getJsonData(clusterSummary);
            String record = jsonObject.toString();
            LOG.debug("get record: {}", record);
            sendRcaClusterSummaryToKafkaQueue(record);
        } else {
            LOG.error("the cluster summary is empty");
        }
    }

    private String sanitizeMessage(String msg) {
        return msg.replaceAll("\\bNaN\\b", "null");
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
