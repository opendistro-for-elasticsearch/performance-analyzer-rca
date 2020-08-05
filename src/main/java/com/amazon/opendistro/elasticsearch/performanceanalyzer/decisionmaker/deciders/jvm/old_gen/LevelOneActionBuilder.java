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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;

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
public class LevelOneActionBuilder extends BaseActionBuilder {

  private LevelOneActionBuilder(final NodeKey esNode, final NodeConfigCache nodeConfigCache) {
    super(esNode, nodeConfigCache);
  }

  public static LevelOneActionBuilder newBuilder(final NodeKey esNode, final NodeConfigCache nodeConfigCache) {
    return new LevelOneActionBuilder(esNode, nodeConfigCache);
  }

  private void addFieldDataCacheAction() {
    Long capacity = NodeConfigCacheReaderUtil
        .readCacheMaxSizeInBytes(nodeConfigCache, esNode, ResourceEnum.FIELD_DATA_CACHE);
    if (capacity == null) {
      return;
    }
    ModifyCacheMaxSizeAction action = new ModifyCacheMaxSizeAction(esNode, ResourceEnum.FIELD_DATA_CACHE, nodeConfigCache,
        capacity, false, LEVEL_ONE_CONST.CACHE_ACTION_STEP_COUNT);
    if (action.isActionable()
        && action.getDesiredCapacityInPercent() >= LEVEL_ONE_CONST.FIELD_DATA_CACHE_LOWER_BOUND) {
      cacheActionMap.put(ResourceEnum.FIELD_DATA_CACHE, action);
    }
  }

  private void addShardRequestCacheAction() {
    Long capacity = NodeConfigCacheReaderUtil
        .readCacheMaxSizeInBytes(nodeConfigCache, esNode, ResourceEnum.SHARD_REQUEST_CACHE);
    if (capacity == null) {
      return;
    }
    ModifyCacheMaxSizeAction action = new ModifyCacheMaxSizeAction(esNode, ResourceEnum.SHARD_REQUEST_CACHE, nodeConfigCache,
        capacity, false, LEVEL_ONE_CONST.CACHE_ACTION_STEP_COUNT);
    if (action.isActionable()
        && action.getDesiredCapacityInPercent() >= LEVEL_ONE_CONST.SHARD_REQUEST_CACHE_LOWER_BOUND) {
      cacheActionMap.put(ResourceEnum.SHARD_REQUEST_CACHE, action);
    }
  }

  @Override
  protected void registerActions() {
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
  @Override
  protected void actionPriorityFilter() {
    actionFilter.put(ResourceEnum.FIELD_DATA_CACHE, true);
    actionFilter.put(ResourceEnum.SHARD_REQUEST_CACHE, true);
  }

  //TODO : read consts from rca.conf
  private static class LEVEL_ONE_CONST {
    public static final double FIELD_DATA_CACHE_LOWER_BOUND = 0.1;
    public static final double SHARD_REQUEST_CACHE_LOWER_BOUND = 0.02;
    public static final int CACHE_ACTION_STEP_COUNT = 1;
  }
}
