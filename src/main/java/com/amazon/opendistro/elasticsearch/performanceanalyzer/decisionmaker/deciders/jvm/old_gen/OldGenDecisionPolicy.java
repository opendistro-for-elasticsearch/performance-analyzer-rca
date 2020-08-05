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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.ArrayList;
import java.util.List;

/**
 * Decision policy for old gen related actions
 *
 * <p>This policy defines 3 levels of unhealthiness — 60-75% (level 1), 75-90% (level 2) and 90%+ (level 3)
 * and create dedicated action builders {@link LevelOneActionBuilder}, {@link LevelTwoActionBuilder},
 * {@link LevelThreeActionBuilder} for each level of unhealthiness
 */
public class OldGenDecisionPolicy {
  private static final double OLD_GEN_THRESHOLD_LEVEL_ONE = 0.6;
  private static final double OLD_GEN_THRESHOLD_LEVEL_TWO = 0.75;
  private static final double OLD_GEN_THRESHOLD_LEVEL_THREE = 0.9;
  private final NodeConfigCache nodeConfigCache;

  public OldGenDecisionPolicy(final NodeConfigCache nodeConfigCache) {
    this.nodeConfigCache = nodeConfigCache;
  }

  public List<Action> actions(final NodeKey esNode, double oldGenUsage) {
    if (oldGenUsage >= OLD_GEN_THRESHOLD_LEVEL_THREE) {
      return LevelThreeActionBuilder.newBuilder(esNode, nodeConfigCache).build();
    }
    else if (oldGenUsage >= OLD_GEN_THRESHOLD_LEVEL_TWO) {
      return LevelTwoActionBuilder.newBuilder(esNode, nodeConfigCache).build();
    }
    else if (oldGenUsage >= OLD_GEN_THRESHOLD_LEVEL_ONE) {
      return LevelOneActionBuilder.newBuilder(esNode, nodeConfigCache).build();
    }
    // old gen jvm is healthy. return empty action list.
    else {
      return new ArrayList<>();
    }
  }
}
