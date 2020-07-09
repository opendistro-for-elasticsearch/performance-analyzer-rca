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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.FieldDataCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.ShardRequestCacheClusterRca;
import java.util.ArrayList;
import java.util.List;

// TODO: 1. Read current cache capacity, total cache capacity, upper bound, lower bound from NodeConfigurationRca

public class CacheHealthDecider extends Decider {

  public static final String NAME = "cache_health";

  private FieldDataCacheClusterRca fieldDataCacheClusterRca;
  private ShardRequestCacheClusterRca shardRequestCacheClusterRca;
  List<String> actionsByUserPriority = new ArrayList<>();
  private int counter = 0;

  public CacheHealthDecider(final long evalIntervalSeconds,
                            final int decisionFrequency,
                            final FieldDataCacheClusterRca fieldDataCacheClusterRca,
                            final ShardRequestCacheClusterRca shardRequestCacheClusterRca) {
    // TODO: Also consume NodeConfigurationRca
    super(evalIntervalSeconds, decisionFrequency);
    this.fieldDataCacheClusterRca = fieldDataCacheClusterRca;
    this.shardRequestCacheClusterRca = shardRequestCacheClusterRca;
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
    getActionsFromRca(fieldDataCacheClusterRca, decision);
    getActionsFromRca(shardRequestCacheClusterRca, decision);
    return decision;
  }

  private <R extends BaseClusterRca> void getActionsFromRca(final R cacheClusterRca, final Decision decision) {
    if (!cacheClusterRca.getFlowUnits().isEmpty()) {
      final HotClusterSummary clusterSummary = cacheClusterRca.getFlowUnits().get(0).getSummary();

      clusterSummary
          .getHotNodeSummaryList()
          .forEach(
              hotNodeSummary -> {
                final NodeKey esNode =
                    new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
                for (final HotResourceSummary resource :
                    hotNodeSummary.getHotResourceSummaryList()) {
                  decision.addAction(
                      computeBestAction(esNode, resource.getResource().getResourceEnum()));
                }
              });
    }
  }

  private void configureActionPriority() {
    // TODO: Input from user configured yml
    this.actionsByUserPriority.add(ModifyCacheCapacityAction.NAME);
  }

  /**
   * Evaluate the most relevant action for a node
   *
   * <p>Action relevance decided based on user configured priorities for now, this can be modified
   * to consume better signals going forward.
   */
  private Action computeBestAction(final NodeKey esNode, final ResourceEnum cacheType) {
    Action action = null;
    for (String actionName : actionsByUserPriority) {
      action =
          getAction(actionName, esNode, cacheType, getNodeCacheCapacity(esNode, cacheType), true);
      if (action != null) {
        break;
      }
    }
    return action;
  }

  private Action getAction(final String actionName,
                           final NodeKey esNode,
                           final ResourceEnum cacheType,
                           final int currentCapacity,
                           final boolean increase) {
    if (ModifyCacheCapacityAction.NAME.equals(actionName)) {
      return configureCacheCapacity(esNode, cacheType, currentCapacity, increase);
    }
    return null;
  }

  private ModifyCacheCapacityAction configureCacheCapacity(final NodeKey esNode,
                                                           final ResourceEnum cacheType,
                                                           final int currentCapacity,
                                                           final boolean increase) {
    final ModifyCacheCapacityAction action = new ModifyCacheCapacityAction(esNode, cacheType, currentCapacity, increase);
    if (action.isActionable()) {
      return action;
    }
    return null;
  }

  private int getNodeCacheCapacity(final NodeKey esNode, final ResourceEnum cacheType) {
    // TODO: use NodeConfigurationRca to return capacity, for now returning random value in MB
    if (cacheType.equals(ResourceEnum.FIELD_DATA_CACHE)) {
      return 10000;
    }
    return 1000;
  }
}
