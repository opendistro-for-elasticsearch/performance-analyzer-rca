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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.DecisionPolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.OldGenDecisionPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Decision policy for old gen related actions
 *
 * <p>This policy defines 3 levels of unhealthiness — 60-75% (level 1), 75-90% (level 2) and 90%+ (level 3)
 * and create dedicated action builders {@link LevelOneActionBuilder}, {@link LevelTwoActionBuilder},
 * {@link LevelThreeActionBuilder} for each level of unhealthiness
 */
public class OldGenDecisionPolicy implements DecisionPolicy {
  private static final Logger LOG = LogManager.getLogger(OldGenDecisionPolicy.class);
  private AppContext appContext;
  private RcaConf rcaConf;
  private final HighHeapUsageClusterRca highHeapUsageClusterRca;

  public OldGenDecisionPolicy(final HighHeapUsageClusterRca highHeapUsageClusterRca) {
    this.highHeapUsageClusterRca = highHeapUsageClusterRca;
  }

  public void setRcaConf(final RcaConf rcaConf) {
    this.rcaConf = rcaConf;
  }

  public void setAppContext(final AppContext appContext) {
    this.appContext = appContext;
  }

  @Override
  public List<Action> evaluate() {
    List<Action> actions = new ArrayList<>();
    if (highHeapUsageClusterRca.getFlowUnits().isEmpty()) {
      return actions;
    }

    ResourceFlowUnit<HotClusterSummary> flowUnit = highHeapUsageClusterRca.getFlowUnits().get(0);
    if (!flowUnit.hasResourceSummary()) {
      return actions;
    }
    HotClusterSummary clusterSummary = flowUnit.getSummary();
    for (HotNodeSummary nodeSummary : clusterSummary.getHotNodeSummaryList()) {
      NodeKey esNode = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());
      for (HotResourceSummary resource : nodeSummary.getHotResourceSummaryList()) {
        if (resource.getResource().equals(ResourceUtil.OLD_GEN_HEAP_USAGE)) {
          actions.addAll(evaluate(esNode, resource.getValue()));
        }
      }
    }
    return actions;
  }

  private List<Action> evaluate(final NodeKey esNode, double oldGenUsage) {
    //rca config / app context will not be null unless there is a bug in RCAScheduler.
    if (rcaConf == null || appContext == null) {
      LOG.error("rca conf/app context is null, return empty action list");
      return new ArrayList<>();
    }
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
