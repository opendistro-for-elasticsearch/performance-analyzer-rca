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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummaryListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterSummaryLogger<T extends GenericSummary> extends Plugin implements ClusterSummaryListener<T> {
  private static final Logger LOG = LogManager.getLogger(ClusterSummaryKafkaPublisher.class);
  private static final String NAME = "Cluster_Summary_Logger";
  private static PluginConfig pluginConfig = null;
  private static KafkaProducer<String, String> kafkaProducerInstance = null;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void summaryPublished(ClusterSummary<T> clusterSummary) {
    LOG.info("Logging updates from clusters: [{}]", clusterSummary.getExistingClusterNameList());
    if (!clusterSummary.summaryMapIsEmpty()) {
      clusterSummary.getSummaryMap().forEach((k, v) -> {
        LOG.info("Cluster name: [{}] , cluster summary: [{}]", k, v);
      });
    } else {
      LOG.info("the cluster summary is empty");
    }
  }
}
