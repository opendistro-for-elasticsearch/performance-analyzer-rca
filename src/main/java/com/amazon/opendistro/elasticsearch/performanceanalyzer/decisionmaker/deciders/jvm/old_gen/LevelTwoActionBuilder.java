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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.CacheActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.QueueActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.ThresholdConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.DeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.WorkLoadTypeConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm.LevelTwoActionBuilderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm.OldGenDecisionPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
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
  private final RcaConf rcaConf;
  private final OldGenDecisionPolicyConfig oldGenDecisionPolicyConfig;
  private final LevelTwoActionBuilderConfig actionBuilderConfig;
  private final WorkLoadTypeConfig workLoadTypeConfig;
  private final CacheActionConfig cacheActionConfig;
  private final QueueActionConfig queueActionConfig;
  private final NodeKey esNode;
  private final Map<ResourceEnum, ModifyCacheMaxSizeAction> cacheActionMap;
  private final Map<ResourceEnum, ModifyQueueCapacityAction> queueActionMap;
  private final Map<ResourceEnum, Boolean> actionFilter;

  private LevelTwoActionBuilder(final NodeKey esNode, final AppContext appContext,
      final RcaConf rcaConf) {
    this.appContext = appContext;
    this.rcaConf = rcaConf;
    DeciderConfig deciderConfig = rcaConf.getDeciderConfig();
    this.oldGenDecisionPolicyConfig = rcaConf.getDeciderConfig().getOldGenDecisionPolicyConfig();
    this.actionBuilderConfig = deciderConfig.getOldGenDecisionPolicyConfig().levelTwoActionBuilderConfig();
    this.workLoadTypeConfig = deciderConfig.getWorkLoadTypeConfig();
    this.cacheActionConfig = rcaConf.getCacheActionConfig();
    this.queueActionConfig = rcaConf.getQueueActionConfig();
    this.esNode = esNode;
    this.cacheActionMap = new HashMap<>();
    this.queueActionMap = new HashMap<>();
    this.actionFilter = new HashMap<>();
    registerActions();
    actionPriorityFilter();
  }

  public static LevelTwoActionBuilder newBuilder(final NodeKey esNode, final AppContext appContext,
      final RcaConf rcaConf) {
    return new LevelTwoActionBuilder(esNode, appContext, rcaConf);
  }

  private void addFieldDataCacheAction() {
    ThresholdConfig<Double> fieldDataCacheConfig = cacheActionConfig.getThresholdConfig(ResourceEnum.FIELD_DATA_CACHE);
    double stepSizeInPercent = OldGenDecisionUtils.calculateStepSize(
        fieldDataCacheConfig.lowerBound(),
        fieldDataCacheConfig.upperBound(),
        oldGenDecisionPolicyConfig.cacheStepCount());

    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction
        .newBuilder(esNode, ResourceEnum.FIELD_DATA_CACHE, appContext, rcaConf)
        .increase(false)
        .stepSizeInPercent(stepSizeInPercent * actionBuilderConfig.fieldDataCacheStepSize())
        .build();
    if (action.isActionable()) {
      cacheActionMap.put(ResourceEnum.FIELD_DATA_CACHE, action);
    }
  }

  private void addShardRequestCacheAction() {
    ThresholdConfig<Double> shardRequestCache = cacheActionConfig.getThresholdConfig(ResourceEnum.SHARD_REQUEST_CACHE);
    double stepSizeInPercent = OldGenDecisionUtils.calculateStepSize(
        shardRequestCache.lowerBound(),
        shardRequestCache.upperBound(),
        oldGenDecisionPolicyConfig.cacheStepCount());

    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction
        .newBuilder(esNode, ResourceEnum.SHARD_REQUEST_CACHE, appContext, rcaConf)
        .increase(false)
        .stepSizeInPercent(stepSizeInPercent * actionBuilderConfig.shardRequestCacheStepSize())
        .build();
    if (action.isActionable()) {
      cacheActionMap.put(ResourceEnum.SHARD_REQUEST_CACHE, action);
    }
  }

  private void addWriteQueueAction() {
    ThresholdConfig<Integer> writeQueueConfig = queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL);
    int stepSize = OldGenDecisionUtils.calculateStepSize(
        writeQueueConfig.lowerBound(),
        writeQueueConfig.upperBound(),
        oldGenDecisionPolicyConfig.queueStepCount());

    ModifyQueueCapacityAction action = ModifyQueueCapacityAction
        .newBuilder(esNode, ResourceEnum.WRITE_THREADPOOL, appContext, rcaConf)
        .increase(false)
        .stepSize(stepSize * actionBuilderConfig.writeQueueStepSize())
        .build();
    if (action.isActionable()) {
      queueActionMap.put(ResourceEnum.WRITE_THREADPOOL, action);
    }
  }

  private void addSearchQueueAction() {
    ThresholdConfig<Integer> searchQueueConfig = queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL);
    int stepSize = OldGenDecisionUtils.calculateStepSize(
        searchQueueConfig.lowerBound(),
        searchQueueConfig.upperBound(),
        oldGenDecisionPolicyConfig.queueStepCount());

    ModifyQueueCapacityAction action = ModifyQueueCapacityAction
        .newBuilder(esNode, ResourceEnum.SEARCH_THREADPOOL, appContext, rcaConf)
        .increase(false)
        .stepSize(stepSize * actionBuilderConfig.searchQueueStepSize())
        .build();
    if (action.isActionable()) {
      queueActionMap.put(ResourceEnum.SEARCH_THREADPOOL, action);
    }
  }


  private void actionPriorityForCache() {
    actionFilter.put(ResourceEnum.FIELD_DATA_CACHE, true);
    actionFilter.put(ResourceEnum.SHARD_REQUEST_CACHE, true);
  }



  // downsize queue based on priority and current queue size
  private void actionPriorityForQueue() {
    NodeConfigCache nodeConfigCache = appContext.getNodeConfigCache();
    Integer writeQueueCapacity = NodeConfigCacheReaderUtil
        .readQueueCapacity(nodeConfigCache, esNode, ResourceEnum.WRITE_THREADPOOL);
    Integer searchQueueCapacity = NodeConfigCacheReaderUtil
        .readQueueCapacity(nodeConfigCache, esNode, ResourceEnum.SEARCH_THREADPOOL);
    if (writeQueueCapacity == null || searchQueueCapacity == null) {
      return;
    }
    ModifyQueueCapacityAction writeQueueAction = queueActionMap.get(ResourceEnum.WRITE_THREADPOOL);
    ModifyQueueCapacityAction searchQueueAction = queueActionMap.get(ResourceEnum.SEARCH_THREADPOOL);
    ThresholdConfig<Integer> writeQueueConfig = queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL);
    ThresholdConfig<Integer> searchQueueConfig = queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL);
    if (writeQueueAction != null && searchQueueAction != null) {
      int writeQueueSizeBucket = OldGenDecisionUtils.bucketization(
          writeQueueConfig.lowerBound(),
          writeQueueConfig.upperBound(),
          writeQueueCapacity,
          oldGenDecisionPolicyConfig.queueBucketSize());
      int searchQueueSizeBucket = OldGenDecisionUtils.bucketization(
          searchQueueConfig.lowerBound(),
          searchQueueConfig.upperBound(),
          searchQueueCapacity,
          oldGenDecisionPolicyConfig.queueBucketSize());
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
