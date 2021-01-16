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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decision;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen.OldGenDecisionPolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.sizing.HeapSizeIncreasePolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.LargeHeapClusterRca;
import java.util.List;

/**
 * decider to bring down heap usage in young gen / old gen
 */
public class HeapHealthDecider extends Decider {

  private static final int EVAL_INTERVAL_IN_S = 5;
  public static final String NAME = "HeapHealthDecider";
  private final OldGenDecisionPolicy oldGenDecisionPolicy;
  private final JvmGenTuningPolicy jvmGenTuningPolicy;
  private final HeapSizeIncreasePolicy heapSizeIncreasePolicy;
  private int counter = 0;

  public HeapHealthDecider(int decisionFrequency,
      final HighHeapUsageClusterRca highHeapUsageClusterRca, LargeHeapClusterRca largeHeapClusterRca) {
    //TODO : refactor parent class to remove evalIntervalSeconds completely
    super(EVAL_INTERVAL_IN_S, decisionFrequency);
    oldGenDecisionPolicy = new OldGenDecisionPolicy(highHeapUsageClusterRca);
    jvmGenTuningPolicy = new JvmGenTuningPolicy(highHeapUsageClusterRca);
    heapSizeIncreasePolicy = new HeapSizeIncreasePolicy(largeHeapClusterRca);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Decision operate() {
    Decision decision = new Decision(System.currentTimeMillis(), NAME);
    counter += 1;
    if (counter < decisionFrequency) {
      return decision;
    }

    counter = 0;
    // oldGenDecisionPolicy are always accepted
    List<Action> oldGenPolicyActions = oldGenDecisionPolicy.evaluate();
    oldGenPolicyActions.forEach(decision::addAction);

    // Add actions from HeapSizeIncreasePolicy (128gb heaps)
    List<Action> jvmScaleUpActions = heapSizeIncreasePolicy.evaluate();
    jvmScaleUpActions.forEach(decision::addAction);
    // If the HeapSizeIncreasePolicy has no suggestions, tune according to the JvmGenTuningPolicy
    if (jvmScaleUpActions == null || jvmScaleUpActions.isEmpty()) {
      List<Action> jvmGenTuningActions = jvmGenTuningPolicy.evaluate();
      jvmGenTuningActions.forEach(decision::addAction);
    }
    return decision;
  }

  @Override
  public void readRcaConf(RcaConf conf) {
    super.readRcaConf(conf);
    oldGenDecisionPolicy.setRcaConf(conf);
    jvmGenTuningPolicy.setRcaConf(conf);
    heapSizeIncreasePolicy.setRcaConf(conf);
  }

  @Override
  public void setAppContext(final AppContext appContext) {
    super.setAppContext(appContext);
    oldGenDecisionPolicy.setAppContext(appContext);
    jvmGenTuningPolicy.setAppContext(appContext);
    heapSizeIncreasePolicy.setAppContext(appContext);
  }
}
