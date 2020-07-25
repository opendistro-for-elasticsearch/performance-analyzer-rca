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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.DeciderActionPriorityReader;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import java.util.ArrayList;
import java.util.List;

// This is a sample decider implementation to finalize decision maker interfaces.
// TODO: 1. Read current queue capacity from NodeConfigurationRca (PR #252)

public class QueueHealthDecider extends Decider {

  public static final String NAME = "queue_health";

  private QueueRejectionClusterRca queueRejectionRca;
  List<String> actionsByUserPriority = null;
  private int counter = 0;

  public QueueHealthDecider(long evalIntervalSeconds, int decisionFrequency, QueueRejectionClusterRca queueRejectionClusterRca) {
    // TODO: Also consume NodeConfigurationRca
    super(evalIntervalSeconds, decisionFrequency);
    this.queueRejectionRca = queueRejectionClusterRca;
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

  public void configureActionPriority(ArrayList<String> priority) {
    actionsByUserPriority = priority;
  }

  /**
   * Evaluate the most relevant action for a node
   *
   * <p>Action relevance decided based on user configured priorities for now, this can be modified
   * to consume better signals going forward.
   */
  private Action computeBestAction(NodeKey esNode, ResourceEnum threadPool) {
    Action action = null;
    for (String actionName : actionsByUserPriority) {
      action =
          getAction(actionName, esNode, threadPool, getNodeQueueCapacity(esNode, threadPool), true);
      if (action != null) {
        break;
      }
    }
    return action;
  }

  private Action getAction(String actionName, NodeKey esNode, ResourceEnum threadPool, int currCapacity, boolean increase) {
    switch (actionName) {
      case ModifyQueueCapacityAction.NAME:
        return configureQueueCapacity(esNode, threadPool, currCapacity, increase);
      default:
        return null;
    }
  }

  private ModifyQueueCapacityAction configureQueueCapacity(NodeKey esNode, ResourceEnum threadPool, int currentCapacity, boolean increase) {
    ModifyQueueCapacityAction action = new ModifyQueueCapacityAction(esNode, threadPool, currentCapacity, increase);
    if (action.isActionable()) {
      return action;
    }
    return null;
  }

  private int getNodeQueueCapacity(NodeKey esNode, ResourceEnum threadPool) {
    // TODO: use NodeConfigurationRca to return capacity, for now returning defaults
    if (threadPool.equals(ResourceEnum.SEARCH_THREADPOOL)) {
      return 1000;
    }
    return 100;
  }
}
