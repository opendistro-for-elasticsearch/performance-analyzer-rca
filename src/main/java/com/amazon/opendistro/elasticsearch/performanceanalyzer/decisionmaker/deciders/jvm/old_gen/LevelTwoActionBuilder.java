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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.WorkLoadTypeConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm.LevelTwoActionBuilderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class LevelTwoActionBuilder {
  private final AppContext appContext;
  private final LevelTwoActionBuilderConfig actionBuilderConfig;
  private final WorkLoadTypeConfig workLoadTypeConfig;
  private final CacheBoundConfig cacheBoundConfig;
  private final ThreadPoolConfig threadPoolConfig;
  private final NodeKey esNode;
  private final Map<ResourceEnum, ModifyCacheMaxSizeAction> cacheActionMap;
  private final Map<ResourceEnum, ModifyQueueCapacityAction> queueActionMap;
  private final Map<ResourceEnum, Boolean> actionFilter;

  private LevelTwoActionBuilder(final NodeKey esNode, final AppContext appContext,
      final DeciderConfig deciderConfig) {
    this.appContext = appContext;
    this.actionBuilderConfig = deciderConfig.getOldGenDecisionPolicyConfig().levelTwoActionBuilderConfig();
    this.workLoadTypeConfig = deciderConfig.getWorkLoadTypeConfig();
    this.cacheBoundConfig = deciderConfig.getCacheBoundConfig();
    this.threadPoolConfig = deciderConfig.getThreadPoolConfig();
    this.esNode = esNode;
    this.cacheActionMap = new HashMap<>();
    this.queueActionMap = new HashMap<>();
    this.actionFilter = new HashMap<>();
    registerActions();
    actionPriorityFilter();
  }

  public static LevelTwoActionBuilder newBuilder(final NodeKey esNode, final AppContext appContext,
      final DeciderConfig deciderConfig) {
    return new LevelTwoActionBuilder(esNode, appContext, deciderConfig);
  }

  private void addFieldDataCacheAction() {
    //TODO : increase step size
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
    //TODO : increase step size
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

  private void addWriteQueueAction() {
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

  /**
   * This function divide the range {lower bound - upper bound } of search/wrire queue into
   * buckets. And allocate the val into its corresponding bucket. The value here refers to the
   * EWMA size of search/write queue. step here is calculated as {range of queue} / {num of buckets}
   * The queue's lower/upper bound can be configured in rca.conf
   */
  private int bucketization(int lowerBound, int upperBound, int val, int bucketSize) {
    double step = (double) (upperBound - lowerBound) / (double) bucketSize;
    return (int) ((double) val / step);
  }

  // downsize queue based on priority and current queue size
  private void actionPriorityForQueue() {
    NodeConfigCache nodeConfigCache = appContext.getNodeConfigCache();
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
      int writeQueueSizeBucket = bucketization(
          threadPoolConfig.writeQueueCapacityLowerBound(),
          threadPoolConfig.writeQueueCapacityUpperBound(),
          writeQueueEWMASize,
          actionBuilderConfig.queueBucketSize());
      int searchQueueSizeBucket = bucketization(
          threadPoolConfig.searchQueueCapacityLowerBound(),
          threadPoolConfig.searchQueueCapacityUpperBound(),
          searchQueueEWMASize,
          actionBuilderConfig.queueBucketSize());
      if (writeQueueSizeBucket > searchQueueSizeBucket) {
        actionFilter.put(ResourceEnum.WRITE_THREADPOOL, true);
      }
      else if (writeQueueSizeBucket < searchQueueSizeBucket) {
        actionFilter.put(ResourceEnum.SEARCH_THREADPOOL, true);
      }
      // tie breaker
      else {
        if (workLoadTypeConfig.preferIngestOverSearch()) {
          actionFilter.put(ResourceEnum.WRITE_THREADPOOL, true);
        }
        else {
          actionFilter.put(ResourceEnum.SEARCH_THREADPOOL, true);
        }
      }
    }
    else if (writeQueueAction != null) {
      actionFilter.put(ResourceEnum.WRITE_THREADPOOL, true);
    }
    else if (searchQueueAction != null) {
      actionFilter.put(ResourceEnum.SEARCH_THREADPOOL, true);
    }
  }

  private void registerActions() {
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
