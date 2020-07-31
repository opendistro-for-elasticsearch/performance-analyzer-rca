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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Collator collects and prunes the candidate decisions from each decider so that their impacts are
 * aligned
 *
 * <p>Decisions can increase or decrease pressure on different key resources on an Elasticearch
 * node. This is encapsulated in each Action via the {@link ImpactVector}. Since each decider
 * independently evaluates its decision, it is possible to have conflicting ImpactVectors from
 * actions across deciders.
 *
 * <p>The collator prunes them to ensure we only take actions that either increase, or decrease
 * pressure on a particular node. To resolve conflicts, we prefer stability over performance.
 */
public class Collator extends Decider {

  public static final String NAME = "collator";

  /* Deciders can choose to publish decisions at different frequencies based on the
   * type of resources monitored and rca signals. The collator should however, not introduce any
   * unnecessary delays. As soon as a decision is taken, it should be evaluated and published downstream.
   */
  private static final int collatorFrequency = 1; // Measured in terms of number of evaluationIntervalPeriods

  private List<Decider> deciders;

  public Collator(long evalIntervalSeconds, Decider... deciders) {
    super(evalIntervalSeconds, collatorFrequency);
    this.deciders = Arrays.asList(deciders);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Decision operate() {
    // This is a simple pass-through collator implementation
    // TODO: Prune actions by their ImpactVectors

    Decision finalDecision = new Decision(System.currentTimeMillis(), NAME);
    for (Decider decider : deciders) {
      Decision decision = decider.getFlowUnits().get(0);
      finalDecision.addAllActions(decision.getActions()
                                          .stream()
                                          .filter(action -> !action.isMuted())
                                          .collect(Collectors.toList()));
    }
    return finalDecision;
  }
}
