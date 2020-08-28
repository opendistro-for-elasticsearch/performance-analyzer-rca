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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummaryListener;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterSummaryKafkaPublisher extends Plugin implements ClusterSummaryListener {
    private static final Logger LOG = LogManager.getLogger(ClusterSummaryKafkaPublisher.class);
    private static final String NAME = "Kafka_Publisher_Plguin";
    private PluginConfig pluginConfig = null;
    private KafkaProducer<String, String> kafkaProducerInstance = null;
    private KafkaProducerController controller;

    public void initialize() {
        if (controller == null) {
            controller = KafkaProducerController.getInstance();
        }
        pluginConfig = controller.getSingletonPluginConfig();
        kafkaProducerInstance = controller.getSingletonKafkaProducer();
    }

    public void sendRcaClusterSummaryToKafkaQueue(String msg) {
        try {
            msg = handleNonNumericNumbers(msg);
            String kafkaTopic = pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.CLUSTER_SUMMARY_KAFKA_TOPIC_KEY);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, msg);
            LOG.info(String.format("sending record: %s to kafka topic: %s ", msg, kafkaTopic));
            kafkaProducerInstance.send(record);
        } catch (KafkaException e) {
            LOG.error("Exception Found on Kafka: " + e.getMessage());
        }
        kafkaProducerInstance.close();
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
        initialize();
        if (!clusterSummary.summaryMapIsEmpty()) {
            LOG.info("Reading updates from clusters: [{}]", clusterSummary.getExistingClusterNameList());
            JsonObject jsonObject = getJsonData(clusterSummary);
            if (jsonObject != null) {
                String record = jsonObject.toString();
                LOG.info("get record: {}", record);
                 sendRcaClusterSummaryToKafkaQueue(record);
            }
        } else {
            LOG.info("the cluster summary is empty");
        }
    }

    private String handleNonNumericNumbers(String msg) {
        return msg.replaceAll("\\bNaN\\b", "null");
    }

    //For testing
    public void setKafkaProducerController(KafkaProducerController testController) {
        controller = testController;
    }
}
