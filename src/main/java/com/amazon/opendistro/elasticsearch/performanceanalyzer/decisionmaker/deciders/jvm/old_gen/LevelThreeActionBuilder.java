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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.DeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.CacheBoundConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.ThreadPoolConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * build actions if old gen falls into level three bucket
 *
 * <p>if old gen usage(after full gc) falls into this bucket 90% - 100%, JVM heap is extremely
 * contended and can run into OOM at any moment. So action builder will build a group of actions
 * to scale down caches to their lower bound in one shot. And for queues we will downsize all
 * queues simultaneously with even higher steps
 */
public class LevelThreeActionBuilder {
  private final AppContext appContext;
  private final NodeKey esNode;
  private final Map<ResourceEnum, ModifyCacheMaxSizeAction> cacheActionMap;
  private final Map<ResourceEnum, ModifyQueueCapacityAction> queueActionMap;
  private final Map<ResourceEnum, Boolean> actionFilter;
  private final CacheBoundConfig cacheBoundConfig;
  private final ThreadPoolConfig threadPoolConfig;

  private LevelThreeActionBuilder(final NodeKey esNode, final AppContext appContext,
      final DeciderConfig deciderConfig) {
    this.appContext = appContext;
    this.esNode = esNode;
    this.cacheActionMap = new HashMap<>();
    this.queueActionMap = new HashMap<>();
    this.actionFilter = new HashMap<>();
    this.cacheBoundConfig = deciderConfig.getCacheBoundConfig();
    this.threadPoolConfig = deciderConfig.getThreadPoolConfig();
    registerActions();
    actionPriorityFilter();
  }

  public static LevelThreeActionBuilder newBuilder(final NodeKey esNode, final AppContext appContext,
      final DeciderConfig deciderConfig) {
    return new LevelThreeActionBuilder(esNode, appContext, deciderConfig);
  }

  //downsize field data cache to its lower bound in one shot
  public void addFieldDataCacheAction() {
    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction
        .newBuilder(esNode, ResourceEnum.FIELD_DATA_CACHE, appContext)
        .increase(false)
        .lowerBoundThreshold(cacheBoundConfig.fieldDataCacheLowerBound())
        .upperBoundThreshold(cacheBoundConfig.fieldDataCacheUpperBound())
        .desiredCacheMaxSize(cacheBoundConfig.fieldDataCacheLowerBound())
        .build();
    if (action.isActionable()) {
      cacheActionMap.put(ResourceEnum.FIELD_DATA_CACHE, action);
    }
  }

  //downsize shard request cache to its lower bound in one shot
  public void addShardRequestCacheAction() {
    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction
        .newBuilder(esNode, ResourceEnum.SHARD_REQUEST_CACHE, appContext)
        .increase(false)
        .lowerBoundThreshold(cacheBoundConfig.shardRequestCacheLowerBound())
        .upperBoundThreshold(cacheBoundConfig.shardRequestCacheUpperBound())
        .desiredCacheMaxSize(cacheBoundConfig.shardRequestCacheLowerBound())
        .build();
    if (action.isActionable()) {
      cacheActionMap.put(ResourceEnum.SHARD_REQUEST_CACHE, action);
    }
  }

  private void addWriteQueueAction() {
    //TODO: increase step size
    ModifyQueueCapacityAction action = ModifyQueueCapacityAction
        .newBuilder(esNode, ResourceEnum.WRITE_THREADPOOL, appContext)
        .increase(false)
        .lowerBound(threadPoolConfig.writeQueueCapacityLowerBound())
        .upperBound(threadPoolConfig.writeQueueCapacityUpperBound())
        .build();
    if (action.isActionable()) {
      queueActionMap.put(ResourceEnum.WRITE_THREADPOOL, action);
    }
  }

  private void addSearchQueueAction() {
    //TODO: increase step size
    ModifyQueueCapacityAction action = ModifyQueueCapacityAction
        .newBuilder(esNode, ResourceEnum.SEARCH_THREADPOOL, appContext)
        .increase(false)
        .lowerBound(threadPoolConfig.searchQueueCapacityLowerBound())
        .upperBound(threadPoolConfig.searchQueueCapacityUpperBound())
        .build();
    if (action.isActionable()) {
      queueActionMap.put(ResourceEnum.SEARCH_THREADPOOL, action);
    }
  }

  private void actionPriorityForCache() {
    actionFilter.put(ResourceEnum.FIELD_DATA_CACHE, true);
    actionFilter.put(ResourceEnum.SHARD_REQUEST_CACHE, true);
  }

  private void actionPriorityForQueue() {
    actionFilter.put(ResourceEnum.WRITE_THREADPOOL, true);
    actionFilter.put(ResourceEnum.SEARCH_THREADPOOL, true);
  }

  private void registerActions() {
    addFieldDataCacheAction();
    addShardRequestCacheAction();
    addSearchQueueAction();
    addWriteQueueAction();
  }

  /**
   * The default priority in this level is
   * 1. downsize both caches simultaneously to its lower bound in one shot.
   * 2. downsize all queues simultaneously until they reach their lower bound
   */
  private void actionPriorityFilter() {
    actionPriorityForCache();
    actionPriorityForQueue();
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
    queueActionMap.forEach((queue, action) -> {
      if (actionFilter.getOrDefault(queue, false)) {
        actions.add(action);
      }
    });
    return actions;
  }
}
