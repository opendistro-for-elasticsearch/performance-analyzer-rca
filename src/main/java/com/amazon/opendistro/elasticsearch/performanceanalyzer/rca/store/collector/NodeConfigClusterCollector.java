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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.EmptyFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.NodeConfigFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Cluster level node config collector that collect node config settings from each node and
 * store them into the {@link NodeConfigCache}
 */
public class NodeConfigClusterCollector extends NonLeafNode<EmptyFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(NodeConfigClusterCollector.class);
  private final NodeConfigCollector nodeConfigCollector;

  public NodeConfigClusterCollector(final NodeConfigCollector nodeConfigCollector) {
    super(0, 5);
    this.nodeConfigCollector = nodeConfigCollector;
  }

  private void addNodeLevelConfigs() {
    List<NodeConfigFlowUnit> flowUnits = nodeConfigCollector.getFlowUnits();
    for (NodeConfigFlowUnit flowUnit : flowUnits) {
      if (flowUnit.isEmpty() || !flowUnit.hasResourceSummary()) {
        continue;
      }
      HotNodeSummary nodeSummary = flowUnit.getSummary();
      NodeKey nodeKey = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());
      NodeConfigCache nodeConfigCache = getAppContext().getNodeConfigCache();
      flowUnit.getConfigList().forEach(resource -> {
        double value = flowUnit.readConfig(resource);
        if (!Double.isNaN(value)) {
          nodeConfigCache.put(nodeKey, resource, value);
        }
      });
    }
  }

  @Override
  public EmptyFlowUnit operate() {
    addNodeLevelConfigs();
    return new EmptyFlowUnit(System.currentTimeMillis());
  }

  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("Collector: Executing fromLocal: {}", name());
    long startTime = System.currentTimeMillis();

    try {
      this.operate();
    } catch (Exception ex) {
      LOG.error("Collector: Exception in operate", ex);
      PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
          ExceptionsAndErrors.EXCEPTION_IN_OPERATE, name(), 1);
    }
    long duration = System.currentTimeMillis() - startTime;

    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
        RcaGraphMetrics.GRAPH_NODE_OPERATE_CALL, this.name(), duration);
  }

  /**
   * NodeConfigClusterCollector does not have downstream nodes and does not emit flow units
   */
  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void handleNodeMuted() {
    assert true;
  }

}
