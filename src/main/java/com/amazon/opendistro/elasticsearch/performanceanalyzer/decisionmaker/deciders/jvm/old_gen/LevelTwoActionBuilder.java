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
 * build actions if old gen falls into level two bucket
 *
 * <p>if old gen usage(after full gc) is between 75% - 90%, it means the JVM starts to become contended and
 * full GC is not able to free up enough objects to bring down heap usage to below 75%.
 * In this case, we will downsize both caches by a larger step size and starts to downsize queues.
 * The action builder in this level will only try to downsize one type of queue at a time. And action priority
 * is used as a tie breaker if both queues are equally important in terms of reducing heap usage..
 *
 * <p>For field data cache, the lower bound in this bucket is 2% of the heap
 * and for shard request cache / query cache, it will be 1% of the heap
 */
public class LevelTwoActionBuilder extends BaseActionBuilder {

  private LevelTwoActionBuilder(final NodeKey esNode, final NodeConfigCache nodeConfigCache) {
    super(esNode, nodeConfigCache);
  }

  public static LevelTwoActionBuilder newBuilder(final NodeKey esNode, final NodeConfigCache nodeConfigCache) {
    return new LevelTwoActionBuilder(esNode, nodeConfigCache);
  }

  private void addFieldDataCacheAction() {
    Long capacity = NodeConfigCacheReaderUtil
        .readCacheMaxSizeInBytes(nodeConfigCache, esNode, ResourceEnum.FIELD_DATA_CACHE);
    if (capacity == null) {
      return;
    }
    ModifyCacheMaxSizeAction action = new ModifyCacheMaxSizeAction(esNode, ResourceEnum.FIELD_DATA_CACHE,
        nodeConfigCache, capacity, false, LEVEL_TWO_CONST.CACHE_ACTION_STEP_COUNT);
    if (action.isActionable()
        && action.getDesiredCapacityInPercent() >= LEVEL_TWO_CONST.FIELD_DATA_CACHE_LOWER_BOUND) {
      cacheActionMap.put(ResourceEnum.FIELD_DATA_CACHE, action);
    }
  }

  private void addShardRequestCacheAction() {
    Long capacity = NodeConfigCacheReaderUtil
        .readCacheMaxSizeInBytes(nodeConfigCache, esNode, ResourceEnum.SHARD_REQUEST_CACHE);
    if (capacity == null) {
      return;
    }
    ModifyCacheMaxSizeAction action = new ModifyCacheMaxSizeAction(esNode, ResourceEnum.SHARD_REQUEST_CACHE,
        nodeConfigCache, capacity, false, LEVEL_TWO_CONST.CACHE_ACTION_STEP_COUNT);
    if (action.isActionable()
        && action.getDesiredCapacityInPercent() >= LEVEL_TWO_CONST.SHARD_REQUEST_CACHE_LOWER_BOUND) {
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
        capacity, false, LEVEL_TWO_CONST.QUEUE_ACTION_STEP_COUNT);
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
        capacity, false, LEVEL_TWO_CONST.QUEUE_ACTION_STEP_COUNT);
    if (action.isActionable()) {
      queueActionMap.put(ResourceEnum.SEARCH_THREADPOOL, action);
    }
  }


  private void actionPriorityForCache() {
    actionFilter.put(ResourceEnum.FIELD_DATA_CACHE, true);
    actionFilter.put(ResourceEnum.SHARD_REQUEST_CACHE, true);
  }

  // allocate value to its bucket
  private int bucketization(int lowerBound, int upperBound, int val, int bucketSize) {
    double step = (double) (upperBound - lowerBound) / (double) bucketSize;
    return (int) ((double) val / step);
  }

  // downsize queue based on priority and current queue size
  private void actionPriorityForQueue() {
    Integer writeQueueEWMASize = NodeConfigCacheReaderUtil
        .readQueueEWMASize(nodeConfigCache, esNode, ResourceEnum.WRITE_THREADPOOL);
    Integer searchQueueEWMASize = NodeConfigCacheReaderUtil
        .readQueueEWMASize(nodeConfigCache, esNode, ResourceEnum.SEARCH_THREADPOOL);
    if (writeQueueEWMASize == null || searchQueueEWMASize == null) {
      return;
    }
    ModifyQueueCapacityAction writeQueueAction = queueActionMap.get(ResourceEnum.WRITE_THREADPOOL);
    ModifyQueueCapacityAction searchQueueAction = queueActionMap.get(ResourceEnum.SEARCH_THREADPOOL);
    if (writeQueueAction != null && searchQueueAction != null) {
      int writeQueueSizeBucket = bucketization(writeQueueAction.getLowerBound(),
          writeQueueAction.getUpperBound(), writeQueueEWMASize, LEVEL_TWO_CONST.QUEUE_SIZE_BUCKET_SIZE);
      int searchQueueSizeBucket = bucketization(searchQueueAction.getLowerBound(),
          searchQueueAction.getUpperBound(), searchQueueEWMASize, LEVEL_TWO_CONST.QUEUE_SIZE_BUCKET_SIZE);
      if (writeQueueSizeBucket > searchQueueSizeBucket) {
        actionFilter.put(ResourceEnum.WRITE_THREADPOOL, true);
      }
      else if (writeQueueSizeBucket < searchQueueSizeBucket) {
        actionFilter.put(ResourceEnum.SEARCH_THREADPOOL, true);
      }
      //tie breaker. default policy prefer writing over indexing
      else {
        actionFilter.put(ResourceEnum.WRITE_THREADPOOL, true);
      }
    }
    else if (writeQueueAction != null) {
      actionFilter.put(ResourceEnum.WRITE_THREADPOOL, true);
    }
    else if (searchQueueAction != null) {
      actionFilter.put(ResourceEnum.SEARCH_THREADPOOL, true);
    }
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
   * 1. downsize both caches simultaneously with larger step size.
   * 2. Allocate size of each queue into buckets and downsize any queue that has
   * higher bucket size.
   * 3. if the bucket size of both queues are equal,
   * the action priority will act as tie breaker and the default setting
   * favors write over search as write is more important in log analytic workload
   * 4. The above default action priority can be overridden by customer settings
   * but customer's acton priority will only affect step 3 above. Step 1 and 2 will
   * be executed regardless of priority action settings
   */
  // TODO : read priority from yml if customer wants to override default ordering
  @Override
  protected void actionPriorityFilter() {
    actionPriorityForCache();
    actionPriorityForQueue();
  }

  //TODO : read consts from rca.conf
  private static class LEVEL_TWO_CONST {
    public static final double FIELD_DATA_CACHE_LOWER_BOUND = 0.02;
    public static final double SHARD_REQUEST_CACHE_LOWER_BOUND = 0.01;
    public static final int CACHE_ACTION_STEP_COUNT = 2;
    public static final int QUEUE_ACTION_STEP_COUNT = 1;
    public static final int QUEUE_SIZE_BUCKET_SIZE = 10;
  }
}
