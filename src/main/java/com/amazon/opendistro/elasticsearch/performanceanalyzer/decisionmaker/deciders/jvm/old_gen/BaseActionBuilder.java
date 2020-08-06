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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseActionBuilder {
  protected final NodeKey esNode;
  protected final NodeConfigCache nodeConfigCache;
  protected final Map<ResourceEnum, ModifyCacheMaxSizeAction> cacheActionMap;
  protected final Map<ResourceEnum, ModifyQueueCapacityAction> queueActionMap;
  protected final Map<ResourceEnum, Boolean> actionFilter;

  BaseActionBuilder(final NodeKey esNode, final NodeConfigCache nodeConfigCache) {
    this.esNode = esNode;
    this.nodeConfigCache = nodeConfigCache;
    this.cacheActionMap = new HashMap<>();
    this.queueActionMap = new HashMap<>();
    this.actionFilter = new HashMap<>();
    registerActions();
    actionPriorityFilter();
  }

  protected abstract void registerActions();

  protected abstract void actionPriorityFilter();

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
