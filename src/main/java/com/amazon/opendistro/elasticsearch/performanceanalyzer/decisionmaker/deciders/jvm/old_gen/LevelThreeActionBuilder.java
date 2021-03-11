/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.CacheClearAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.CacheActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.QueueActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.DeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.LevelThreeActionBuilderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.OldGenDecisionPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.ArrayList;
import java.util.List;

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
  private final RcaConf rcaConf;
  private final NodeKey esNode;
  private final List<Action> actions;
  private final OldGenDecisionPolicyConfig oldGenDecisionPolicyConfig;
  private final LevelThreeActionBuilderConfig actionBuilderConfig;
  private final CacheActionConfig cacheActionConfig;
  private final QueueActionConfig queueActionConfig;

  private LevelThreeActionBuilder(final NodeKey esNode, final AppContext appContext,
      final RcaConf rcaConf) {
    this.appContext = appContext;
    this.rcaConf = rcaConf;
    this.esNode = esNode;
    DeciderConfig deciderConfig = rcaConf.getDeciderConfig();
    this.oldGenDecisionPolicyConfig = rcaConf.getDeciderConfig().getOldGenDecisionPolicyConfig();
    this.actionBuilderConfig = deciderConfig.getOldGenDecisionPolicyConfig().levelThreeActionBuilderConfig();
    this.cacheActionConfig = rcaConf.getCacheActionConfig();
    this.queueActionConfig = rcaConf.getQueueActionConfig();
    this.actions = new ArrayList<>();
  }

  public static LevelThreeActionBuilder newBuilder(final NodeKey esNode, final AppContext appContext,
      final RcaConf rcaConf) {
    return new LevelThreeActionBuilder(esNode, appContext, rcaConf);
  }

  //downsize field data cache to its lower bound in one shot
  public void addFieldDataCacheAction() {
    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction
        .newBuilder(esNode, ResourceEnum.FIELD_DATA_CACHE, appContext, rcaConf)
        .increase(false)
        .setDesiredCacheMaxSizeToMin()
        .build();
    if (action.isActionable()) {
      actions.add(action);
    }
  }

  //downsize shard request cache to its lower bound in one shot
  public void addShardRequestCacheAction() {
    ModifyCacheMaxSizeAction action = ModifyCacheMaxSizeAction
        .newBuilder(esNode, ResourceEnum.SHARD_REQUEST_CACHE, appContext, rcaConf)
        .increase(false)
        .setDesiredCacheMaxSizeToMin()
        .build();
    if (action.isActionable()) {
      actions.add(action);
    }
  }

  private void addWriteQueueAction() {
    int stepSize = queueActionConfig.getStepSize(ResourceEnum.WRITE_THREADPOOL);

    ModifyQueueCapacityAction action = ModifyQueueCapacityAction
        .newBuilder(esNode, ResourceEnum.WRITE_THREADPOOL, appContext, rcaConf)
        .increase(false)
        .stepSize(stepSize * actionBuilderConfig.writeQueueStepSize())
        .build();
    if (action.isActionable()) {
      actions.add(action);
    }
  }

  private void addSearchQueueAction() {
    int stepSize = queueActionConfig.getStepSize(ResourceEnum.SEARCH_THREADPOOL);

    ModifyQueueCapacityAction action = ModifyQueueCapacityAction
        .newBuilder(esNode, ResourceEnum.SEARCH_THREADPOOL, appContext, rcaConf)
        .increase(false)
        .stepSize(stepSize * actionBuilderConfig.searchQueueStepSize())
        .build();
    if (action.isActionable()) {
      actions.add(action);
    }
  }

  private void addCacheClearAction() {
    CacheClearAction action = CacheClearAction
        .newBuilder(appContext).build();
    if (action.isActionable()) {
      actions.add(action);
    }
  }

  /**
   * build actions.
   * @return List of actions
   */
  public List<Action> build() {
    addFieldDataCacheAction();
    addShardRequestCacheAction();
    addSearchQueueAction();
    addWriteQueueAction();
    addCacheClearAction();
    return actions;
  }
}
