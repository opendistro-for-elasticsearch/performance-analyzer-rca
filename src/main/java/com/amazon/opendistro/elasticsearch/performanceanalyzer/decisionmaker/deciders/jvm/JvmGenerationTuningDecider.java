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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decision;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;

/**
 * JvmGenerationTuningDecider tunes the sizing ratio between the old and young generations
 */
public class JvmGenerationTuningDecider extends Decider {
  private final HighHeapUsageClusterRca highHeapUsageClusterRca;
  private final JvmGenerationTuningPolicy jvmGenerationTuningPolicy;
  private int counter = 0;

  public JvmGenerationTuningDecider(int decisionFrequency, final HighHeapUsageClusterRca highHeapUsageClusterRca) {
    super(5, decisionFrequency);
    this.highHeapUsageClusterRca = highHeapUsageClusterRca;
    jvmGenerationTuningPolicy = new JvmGenerationTuningPolicy();
  }

  @Override
  public String name() {
    return getClass().getSimpleName();
  }

  private void recordIssues() {
    if (highHeapUsageClusterRca.getFlowUnits().isEmpty()) {
      return;
    }

    ResourceFlowUnit<HotClusterSummary> flowUnit = highHeapUsageClusterRca.getFlowUnits().get(0);
    if (!flowUnit.hasResourceSummary()) {
      return;
    }

    HotClusterSummary clusterSummary = flowUnit.getSummary();
    for (HotNodeSummary nodeSummary : clusterSummary.getHotNodeSummaryList()) {
      for (HotResourceSummary summary : nodeSummary.getHotResourceSummaryList()) {
        jvmGenerationTuningPolicy.record(summary);
      }
    }
  }

  @Override
  public Decision operate() {
    Decision decision = new Decision(System.currentTimeMillis(), name());
    counter += 1;
    if (counter < decisionFrequency) {
      return decision;
    } else {
      counter = 0;
    }
    recordIssues();
    decision.addAllActions(jvmGenerationTuningPolicy.actions());
    return decision;
  }

  @Override
  public void setAppContext(AppContext appContext) {
    super.setAppContext(appContext);
    jvmGenerationTuningPolicy.setAppContext(appContext);
  }
}
