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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.DeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.CacheBoundConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm.LevelOneActionBuilderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * build actions if old gen falls into level one bucket
 *
 * <p>if old gen usage(after full gc) is between 60% - 75%, it has not yet reach the 75% threshold in GC.
 * So we will downsize each cache by regular step size and leave the queues untouched. By default Actions in this
 * bucket will ignore priority settings of caches and downsize both caches simultaneously until the capacity
 * of those caches reaches the lower bounds.
 *
 * <p>For field data cache, the lower bound in this bucket is 10% of the heap
 * and for shard request cache / query cache, it will be 2% of the heap(default ES settings). This is
 * to free up excessive heap used by fielddata cache or query cache because JVM decider favors stability
 * over performance.
 */
public class LevelOneActionBuilder {
  private final AppContext appContext;
  private final LevelOneActionBuilderConfig actionBuilderConfig;
  private final CacheBoundConfig cacheBoundConfig;
  private final NodeKey esNode;
  private final Map<ResourceEnum, ModifyCacheMaxSizeAction> cacheActionMap;
  private final Map<ResourceEnum, Boolean> actionFilter;

  private LevelOneActionBuilder(final NodeKey esNode, final AppContext appContext, final
  DeciderConfig deciderConfig) {
    this.appContext = appContext;
    this.actionBuilderConfig = deciderConfig.getOldGenDecisionPolicyConfig().levelOneActionBuilderConfig();
    this.cacheBoundConfig = deciderConfig.getCacheBoundConfig();
    this.esNode = esNode;
    this.cacheActionMap = new HashMap<>();
    this.actionFilter = new HashMap<>();
    registerActions();
    actionPriorityFilter();
  }

  public static LevelOneActionBuilder newBuilder(final NodeKey esNode, final AppContext appContext, final
  DeciderConfig deciderConfig) {
    return new LevelOneActionBuilder(esNode, appContext, deciderConfig);
  }

  private void addFieldDataCacheAction() {
    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction
        .newBuilder(esNode, ResourceEnum.FIELD_DATA_CACHE, appContext)
        .increase(false)
        .lowerBoundThreshold(actionBuilderConfig.fieldDataCacheLowerBound())
        .upperBoundThreshold(cacheBoundConfig.fieldDataCacheUpperBound())
        .build();
    if (action.isActionable()) {
      cacheActionMap.put(ResourceEnum.FIELD_DATA_CACHE, action);
    }
  }

  private void addShardRequestCacheAction() {
    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction
        .newBuilder(esNode, ResourceEnum.SHARD_REQUEST_CACHE, appContext)
        .increase(false)
        .lowerBoundThreshold(actionBuilderConfig.shardRequestCacheLowerBound())
        .upperBoundThreshold(cacheBoundConfig.shardRequestCacheUpperBound())
        .build();
    if (action.isActionable()) {
      cacheActionMap.put(ResourceEnum.SHARD_REQUEST_CACHE, action);
    }
  }

  private void registerActions() {
    addFieldDataCacheAction();
    addShardRequestCacheAction();
  }

  /**
   * generate final action list based on action priority.
   * The default priority in this level is to downsize both caches simultaneously
   * unless explicitly overridden by customer yml.
   * @return final action list based on action priority
   */
  // TODO : read priority from yml if customer wants to override default ordering
  private void actionPriorityFilter() {
    actionFilter.put(ResourceEnum.FIELD_DATA_CACHE, true);
    actionFilter.put(ResourceEnum.SHARD_REQUEST_CACHE, true);
  }

  /**
   * build actions.
   * @return List of actions
   */
  public List<Action> build() {
    List<Action> actions = new ArrayList<>();
    cacheActionMap.forEach((cache, action) -> {
      if (actionFilter.getOrDefault(cache, false)) {
        actions.add(action);
      }
    });
    return actions;
  }
}
