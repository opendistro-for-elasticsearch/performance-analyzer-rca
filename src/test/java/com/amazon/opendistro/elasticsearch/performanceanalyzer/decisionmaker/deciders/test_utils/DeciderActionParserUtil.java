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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.test_utils;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.CacheClearAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeciderActionParserUtil {
  private final Map<ResourceEnum, ModifyCacheMaxSizeAction> cacheActionMap;
  private final Map<ResourceEnum, ModifyQueueCapacityAction> queueActionMap;
  private CacheClearAction cacheClearAction;

  public DeciderActionParserUtil() {
    cacheActionMap = new HashMap<>();
    queueActionMap = new HashMap<>();
    cacheClearAction = null;
  }

  public void addActions(List<Action> actions) throws IllegalArgumentException {
    cacheActionMap.clear();
    queueActionMap.clear();
    for (Action action : actions) {
      if (action instanceof ModifyQueueCapacityAction) {
        ModifyQueueCapacityAction queueAction = (ModifyQueueCapacityAction) action;
        assert !queueActionMap.containsKey(queueAction.getThreadPool());
        queueActionMap.put(queueAction.getThreadPool(), queueAction);
      }
      else if (action instanceof ModifyCacheMaxSizeAction) {
        ModifyCacheMaxSizeAction cacheAction = (ModifyCacheMaxSizeAction) action;
        assert !cacheActionMap.containsKey(cacheAction.getCacheType());
        cacheActionMap.put(cacheAction.getCacheType(), cacheAction);
      }
      else if (action instanceof CacheClearAction) {
        assert cacheClearAction == null;
        cacheClearAction = (CacheClearAction) action;
      }
      else {
        assert false;
      }
    }
  }

  public ModifyCacheMaxSizeAction readCacheAction(ResourceEnum resource) {
    return cacheActionMap.getOrDefault(resource, null);
  }

  public ModifyQueueCapacityAction readQueueAction(ResourceEnum resource) {
    return queueActionMap.getOrDefault(resource, null);
  }

  public CacheClearAction readCacheClearAction() {
    return cacheClearAction;
  }

  public int size() {
    return cacheActionMap.size()
        + queueActionMap.size()
        + (cacheClearAction == null ? 0 : 1);
  }
}
