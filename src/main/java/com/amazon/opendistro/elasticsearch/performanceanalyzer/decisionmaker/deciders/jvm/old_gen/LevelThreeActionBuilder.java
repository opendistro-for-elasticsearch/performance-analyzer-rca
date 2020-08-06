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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;

/**
 * build actions if old gen falls into level three bucket
 *
 * <p>if old gen usage(after full gc) falls into this bucket 90% - 100%, JVM heap is extremely
 * contended and can run into OOM at any moment. So action builder will build a group of actions
 * to scale down caches to their lower bound in one shot. And for queues we will downsize all
 * queues simultaneously with even higher steps
 */
public class LevelThreeActionBuilder extends BaseActionBuilder {

  private LevelThreeActionBuilder(final NodeKey esNode, final NodeConfigCache nodeConfigCache) {
    super(esNode, nodeConfigCache);
  }

  public static LevelThreeActionBuilder newBuilder(final NodeKey esNode, final NodeConfigCache nodeConfigCache) {
    return new LevelThreeActionBuilder(esNode, nodeConfigCache);
  }

  //downsize field data cache to its lower bound in one shot
  public void addFieldDataCacheAction() {
    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction.newMinimalCapacityAction(esNode,
        ResourceEnum.FIELD_DATA_CACHE, nodeConfigCache, 1.0);
    if (action.isActionable()) {
      cacheActionMap.put(ResourceEnum.FIELD_DATA_CACHE, action);
    }
  }

  //downsize shard request cache to its lower bound in one shot
  public void addShardRequestCacheAction() {
    ModifyCacheMaxSizeAction action =  ModifyCacheMaxSizeAction.newMinimalCapacityAction(esNode,
        ResourceEnum.SHARD_REQUEST_CACHE, nodeConfigCache, 1.0);
    if (action.isActionable()) {
      cacheActionMap.put(ResourceEnum.SHARD_REQUEST_CACHE, action);
    }
  }

  private void addWriteQueueAction() {
    Integer capacity = NodeConfigCacheReaderUtil
        .readQueueCapacity(nodeConfigCache, esNode, ResourceEnum.WRITE_THREADPOOL);
    if (capacity == null) {
      return;
    }
    ModifyQueueCapacityAction action = new ModifyQueueCapacityAction(esNode, ResourceEnum.WRITE_THREADPOOL,
        capacity, false, LEVEL_THREE_CONST.QUEUE_ACTION_STEP_COUNT);
    if (action.isActionable()) {
      queueActionMap.put(ResourceEnum.WRITE_THREADPOOL, action);
    }
  }

  private void addSearchQueueAction() {
    Integer capacity = NodeConfigCacheReaderUtil
        .readQueueCapacity(nodeConfigCache, esNode, ResourceEnum.SEARCH_THREADPOOL);
    if (capacity == null) {
      return;
    }
    ModifyQueueCapacityAction action = new ModifyQueueCapacityAction(esNode, ResourceEnum.SEARCH_THREADPOOL,
        capacity, false, LEVEL_THREE_CONST.QUEUE_ACTION_STEP_COUNT);
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

  @Override
  protected void registerActions() {
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
  // TODO : read priority from yml if customer wants to override default ordering
  @Override
  protected void actionPriorityFilter() {
    actionPriorityForCache();
    actionPriorityForQueue();
  }

  private static class LEVEL_THREE_CONST {
    public static final int QUEUE_ACTION_STEP_COUNT = 2;
  }
}
