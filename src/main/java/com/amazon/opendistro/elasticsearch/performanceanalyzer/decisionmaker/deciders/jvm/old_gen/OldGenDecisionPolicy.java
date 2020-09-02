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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm.OldGenDecisionPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
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
  private final AppContext appContext;
  private final RcaConf rcaConf;

  public OldGenDecisionPolicy(final AppContext appContext, final RcaConf rcaConf) {
    this.appContext = appContext;
    //decider config will not be null unless there is a bug in RCAScheduler.
    assert rcaConf != null : "DeciderConfig is null";
    this.rcaConf = rcaConf;
  }

  public List<Action> actions(final NodeKey esNode, double oldGenUsage) {
    OldGenDecisionPolicyConfig oldGenDecisionPolicyConfig =
        rcaConf.getDeciderConfig().getOldGenDecisionPolicyConfig();
    if (oldGenUsage >= oldGenDecisionPolicyConfig.oldGenThresholdLevelThree()) {
      return LevelThreeActionBuilder.newBuilder(esNode, appContext, rcaConf).build();
    }
    else if (oldGenUsage >= oldGenDecisionPolicyConfig.oldGenThresholdLevelTwo()) {
      return LevelTwoActionBuilder.newBuilder(esNode, appContext, rcaConf).build();
    }
    else if (oldGenUsage >= oldGenDecisionPolicyConfig.oldGenThresholdLevelOne()) {
      return LevelOneActionBuilder.newBuilder(esNode, appContext, rcaConf).build();
    }
    // old gen jvm is healthy. return empty action list.
    else {
      return new ArrayList<>();
    }
  }
}
