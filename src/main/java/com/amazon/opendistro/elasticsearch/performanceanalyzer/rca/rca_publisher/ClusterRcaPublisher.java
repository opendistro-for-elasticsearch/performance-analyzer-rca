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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.EmptyFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClusterRcaPublisher extends NonLeafNode<EmptyFlowUnit> {
  private static final Logger LOG = LogManager.getLogger(ClusterRcaPublisher.class);
  public static final String NAME = "ClusterReaderRca";
  private final List<Rca<ResourceFlowUnit<HotClusterSummary>>> clusterRcas;
  private ClusterSummary clusterSummary;
  private List<ClusterSummaryListener> clusterSummaryListeners;

  public ClusterRcaPublisher(final int evalIntervalSeconds, List<Rca<ResourceFlowUnit<HotClusterSummary>>> clusterRcas) {
    super(0, evalIntervalSeconds);
    this.clusterRcas = clusterRcas;
    clusterSummary = new ClusterSummary(evalIntervalSeconds, new HashMap<>());
    clusterSummaryListeners = new ArrayList<>();
    LOG.info("Creating new ClusterRcaPublisher");
  }

  public String name() {
    return NAME;
  }

  public String getAllClusterRcaName() {
    StringBuilder list = new StringBuilder();
    clusterRcas.forEach((cluster) -> {
      list.append(cluster.name()).append(", ");
    });
    return list.toString();
  }

  public void addClusterRca(Rca<ResourceFlowUnit<HotClusterSummary>> rca) {
    clusterRcas.add(rca);
  }

  public ClusterSummary getClusterSummary() {
    return clusterSummary;
  }

  public List<ClusterSummaryListener> getClusterSummaryListeners() {
    return clusterSummaryListeners;
  }

  public void addClusterSummaryListener(ClusterSummaryListener listener) {
    clusterSummaryListeners.add(listener);
  }

  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("ClusterReaderRca: reading Rca data from: {}", getAllClusterRcaName());
    long startTime = System.currentTimeMillis();
    try {
      this.operate();
    } catch (Exception e) {
      LOG.error("ClusterReaderRca: Exception in operate. ", e);
      PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
              ExceptionsAndErrors.EXCEPTION_IN_OPERATE, name(), 1);
    }
    long duration = System.currentTimeMillis() - startTime;
    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
            RcaGraphMetrics.GRAPH_NODE_OPERATE_CALL, name(), duration);
  }

  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
            + "be required.");
  }

  @Override
  public void handleNodeMuted() {
    assert true;
  }

  @Override
  public EmptyFlowUnit operate() {
    LOG.info("operating");
    for (Rca<ResourceFlowUnit<HotClusterSummary>> clusterRca : clusterRcas) {
      List<ResourceFlowUnit<HotClusterSummary>> clusterFlowUnits = clusterRca.getFlowUnits();
      if (clusterFlowUnits.isEmpty()) {
        LOG.info("cluster flow unit is empty");
        continue;
      }
      if (clusterFlowUnits.get(0).hasResourceSummary()) {
        LOG.info("adding summary \"{}\" to cluster: {}", clusterRca.getFlowUnits().get(0).getSummary().toString(), clusterRca.name());
        clusterSummary.addValidSummary(clusterRca.name(), clusterRca.getFlowUnits().get(0).getSummary(), System.currentTimeMillis());
      }
    }
    for (ClusterSummaryListener listener : clusterSummaryListeners) {
      listener.summaryPublished(clusterSummary);
    }
    return new EmptyFlowUnit(System.currentTimeMillis());
  }
}
