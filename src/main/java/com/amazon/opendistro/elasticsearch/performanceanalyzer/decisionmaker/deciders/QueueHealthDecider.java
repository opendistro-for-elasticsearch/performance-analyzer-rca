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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// This is a sample decider implementation to finalize decision maker interfaces.
// TODO: 1. Read action priorities from a configurable yml

public class QueueHealthDecider extends HeapBasedDecider {

  private static final Logger LOG = LogManager.getLogger(Decider.class);
  public static final String NAME = "queue_health";

  private QueueRejectionClusterRca queueRejectionRca;
  List<String> actionsByUserPriority = new ArrayList<>();
  private int counter = 0;

  public QueueHealthDecider(long evalIntervalSeconds, int decisionFrequency, QueueRejectionClusterRca queueRejectionClusterRca,
                            HighHeapUsageClusterRca highHeapUsageClusterRca) {
    super(evalIntervalSeconds, decisionFrequency, highHeapUsageClusterRca);
    this.queueRejectionRca = queueRejectionClusterRca;
    configureActionPriority();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Decision operate() {
    Decision decision = new Decision(System.currentTimeMillis(), NAME);
    counter += 1;
    if (counter < decisionFrequency) {
      return decision;
    }

    counter = 0;
    if (queueRejectionRca.getFlowUnits().isEmpty()) {
      return decision;
    }

    ResourceFlowUnit<HotClusterSummary> flowUnit = queueRejectionRca.getFlowUnits().get(0);
    if (!flowUnit.hasResourceSummary()) {
      return decision;
    }
    HotClusterSummary clusterSummary = flowUnit.getSummary();
    for (HotNodeSummary nodeSummary : clusterSummary.getHotNodeSummaryList()) {
      NodeKey esNode = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());
      for (HotResourceSummary resource : nodeSummary.getHotResourceSummaryList()) {
        decision.addAction(computeBestAction(esNode, resource.getResource().getResourceEnum()));
      }
    }
    return decision;
  }

  private void configureActionPriority() {
    // TODO: Input from user configured yml
    this.actionsByUserPriority.add(ModifyQueueCapacityAction.NAME);
  }

  /**
   * Evaluate the most relevant action for a node
   *
   * <p>Action relevance decided based on user configured priorities for now, this can be modified
   * to consume better signals going forward.
   */
  private Action computeBestAction(NodeKey esNode, ResourceEnum threadPool) {
    Action action = null;
    if (canUseMoreHeap(esNode)) {
      for (String actionName : actionsByUserPriority) {
        action = getAction(actionName, esNode, threadPool, true);
        if (action != null) {
          break;
        }
      }
    } else {
      PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
          RcaRuntimeMetrics.NO_INCREASE_ACTION_SUGGESTED, NAME + ":" + esNode.getHostAddress(), 1);
    }
    return action;
  }

  private Action getAction(String actionName, NodeKey esNode, ResourceEnum threadPool, boolean increase) {
    switch (actionName) {
      case ModifyQueueCapacityAction.NAME:
        return configureQueueCapacity(esNode, threadPool, increase);
      default:
        return null;
    }
  }

  private ModifyQueueCapacityAction configureQueueCapacity(NodeKey esNode, ResourceEnum threadPool, boolean increase) {
    ModifyQueueCapacityAction action = ModifyQueueCapacityAction
        .newBuilder(esNode, threadPool, getAppContext(), rcaConf)
        .increase(increase)
        .build();
    if (action != null && action.isActionable()) {
      return action;
    }
    return null;
  }
}
