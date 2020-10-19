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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.FieldDataCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.ShardRequestCacheClusterRca;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO: 1. Create separate ActionConfig objects for different actions

public class CacheHealthDecider extends HeapBasedDecider {
  private static final Logger LOG = LogManager.getLogger(CacheHealthDecider.class);
  public static final String NAME = "cacheHealthDecider";

  private final FieldDataCacheClusterRca fieldDataCacheClusterRca;
  private final ShardRequestCacheClusterRca shardRequestCacheClusterRca;
  private final ImmutableMap<ResourceEnum, BaseClusterRca> cacheTypeBaseClusterRcaMap;

  List<ResourceEnum> modifyCacheActionPriorityList = new ArrayList<>();
  private int counter = 0;

  public CacheHealthDecider(
      final long evalIntervalSeconds,
      final int decisionFrequency,
      final FieldDataCacheClusterRca fieldDataCacheClusterRca,
      final ShardRequestCacheClusterRca shardRequestCacheClusterRca,
      final HighHeapUsageClusterRca highHeapUsageClusterRca) {
    super(evalIntervalSeconds, decisionFrequency, highHeapUsageClusterRca);
    configureModifyCacheActionPriority();

    this.fieldDataCacheClusterRca = fieldDataCacheClusterRca;
    this.shardRequestCacheClusterRca = shardRequestCacheClusterRca;
    this.cacheTypeBaseClusterRcaMap =
        ImmutableMap.<ResourceEnum, BaseClusterRca>builder()
            .put(ResourceEnum.SHARD_REQUEST_CACHE, shardRequestCacheClusterRca)
            .put(ResourceEnum.FIELD_DATA_CACHE, fieldDataCacheClusterRca)
            .build();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Decision operate() {
    final Set<InstanceDetails.Id> impactedNodes = new HashSet<>();

    Decision decision = new Decision(System.currentTimeMillis(), NAME);
    counter += 1;
    if (counter < decisionFrequency) {
      return decision;
    }
    counter = 0;

    for (final ResourceEnum cacheType : modifyCacheActionPriorityList) {
      getActionsFromRca(cacheTypeBaseClusterRcaMap.get(cacheType), impactedNodes).forEach(decision::addAction);
    }
    return decision;
  }

  private <R extends BaseClusterRca> List<Action> getActionsFromRca(
      final R cacheClusterRca,
      final Set<InstanceDetails.Id> impactedNodes) {
    final List<Action> actions = new ArrayList<>();

    if (!cacheClusterRca.getFlowUnits().isEmpty()) {
      final ResourceFlowUnit<HotClusterSummary> flowUnit = cacheClusterRca.getFlowUnits().get(0);
      if (!flowUnit.hasResourceSummary()) {
        return actions;
      }

      final List<HotNodeSummary> clusterSummary = flowUnit.getSummary().getHotNodeSummaryList();

      for (final HotNodeSummary hotNodeSummary : clusterSummary) {
        if (!impactedNodes.contains(hotNodeSummary.getNodeID())) {
          final NodeKey esNode = new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
          for (final HotResourceSummary resource : hotNodeSummary.getHotResourceSummaryList()) {
            final Action action = computeBestAction(esNode, resource.getResource().getResourceEnum());
            if (action != null) {
              actions.add(action);
              impactedNodes.add(hotNodeSummary.getNodeID());
            }
          }
        }
      }
    }
    return actions;
  }

  private void configureModifyCacheActionPriority() {
    // Assigning shard request cache higher priority over field data cache
    // TODO: Modify as per the performance test results
    this.modifyCacheActionPriorityList.add(ResourceEnum.SHARD_REQUEST_CACHE);
    this.modifyCacheActionPriorityList.add(ResourceEnum.FIELD_DATA_CACHE);
  }

  /**
   * Evaluate the most relevant action for a node for the specific cache type
   *
   * <p>Only ModifyCacheMaxSize Action is used for now, this can be modified to consume better
   * signals going forward.
   */
  private Action computeBestAction(final NodeKey esNode, final ResourceEnum cacheType) {
    Action action = null;
    if (canUseMoreHeap(esNode)) {
      action = getAction(ModifyCacheMaxSizeAction.NAME, esNode, cacheType, true);
    } else {
      PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
          RcaRuntimeMetrics.NO_INCREASE_ACTION_SUGGESTED, NAME + ":" + esNode.getHostAddress(), 1);
    }
    return action;
  }

  private Action getAction(
      final String actionName, final NodeKey esNode, final ResourceEnum cacheType, final boolean increase) {
    if (ModifyCacheMaxSizeAction.NAME.equals(actionName)) {
      return configureCacheMaxSize(esNode, cacheType, increase);
    }
    return null;
  }

  private ModifyCacheMaxSizeAction configureCacheMaxSize(
      final NodeKey esNode, final ResourceEnum cacheType, final boolean increase) {
    final ModifyCacheMaxSizeAction action =
        ModifyCacheMaxSizeAction
            .newBuilder(esNode, cacheType, getAppContext(), rcaConf)
            .increase(increase)
            .build();
    if (action.isActionable()) {
      return action;
    }
    return null;
  }
}
