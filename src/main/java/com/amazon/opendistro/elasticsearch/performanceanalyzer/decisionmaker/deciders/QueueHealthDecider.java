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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.DecisionMakerConsts.HEAP_TUNABLE_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.DecisionMakerConsts.HEAP_USAGE_MAP;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.util.HeapUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BasicBucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// This is a sample decider implementation to finalize decision maker interfaces.
// TODO: 1. Read action priorities from a configurable yml

public class QueueHealthDecider extends Decider {

  private static final Logger LOG = LogManager.getLogger(Decider.class);
  public static final String NAME = "queue_health";

  private QueueRejectionClusterRca queueRejectionRca;
  List<String> actionsByUserPriority = new ArrayList<>();
  private BucketCalculator heapUsageBucketCalculator;
  private int counter = 0;

  public QueueHealthDecider(long evalIntervalSeconds, int decisionFrequency, QueueRejectionClusterRca queueRejectionClusterRca) {
    super(evalIntervalSeconds, decisionFrequency);
    this.queueRejectionRca = queueRejectionClusterRca;
    configureActionPriority();
    this.heapUsageBucketCalculator = new BasicBucketCalculator(HEAP_USAGE_MAP);
    try {
      this.heapUsageBucketCalculator = rcaConf.getBucketizationSettings(HEAP_TUNABLE_NAME);
    } catch (final Exception e) {
      LOG.warn("Heap usage tunable does not exist in rca.conf. Using the default values.");
    }
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
    final NodeConfigCache nodeConfigCache = getAppContext().getNodeConfigCache();
    double heapUsedPercent = HeapUtil.getHeapUsedPercentage(
            NodeConfigCacheReaderUtil.readHeapMaxSizeInBytes(nodeConfigCache, esNode),
            NodeConfigCacheReaderUtil.readHeapUsageInBytes(nodeConfigCache, esNode));
    Action action = null;

    if (heapUsageBucketCalculator.compute(heapUsedPercent).equals(UsageBucket.HEALTHY_WITH_BUFFER)
                || heapUsageBucketCalculator.compute(heapUsedPercent).equals(UsageBucket.UNDER_UTILIZED)) {
      for (String actionName : actionsByUserPriority) {
        action =
                getAction(actionName, esNode, threadPool, true);
        if (action != null) {
          break;
        }
      }
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
