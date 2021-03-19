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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decision;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Collator collects and prunes the candidate decisions from each decider so that their impacts are
 * aligned.
 *
 * <p>Decisions can increase or decrease pressure on different key resources on an Elasticsearch
 * node. This is encapsulated in each Action via the {@link ImpactVector}. Since each decider
 * independently evaluates its decision, it is possible to have conflicting ImpactVectors from
 * actions across deciders.
 *
 * <p>The collator prunes them to ensure we only take actions that either increase, or decrease
 * pressure on a particular node's resources. To resolve conflicts, we prefer stability over
 * performance. In order for the above guarantee to work, there should be only one collator instance
 * in an {@link AnalysisGraph}.
 */
public class Collator extends Decider {

  public static final String NAME = "collator";

  /* Deciders can choose to publish decisions at different frequencies based on the
   * type of resources monitored and rca signals. The collator should however, not introduce any
   * unnecessary delays. As soon as a decision is taken, it should be evaluated and published downstream.
   */
  private static final int collatorFrequency = 1; // Measured in terms of number of evaluationIntervalPeriods

  private static final int evalIntervalSeconds = 5;

  private final ImpactAssessor impactAssessor;

  private final List<Decider> deciders;

  private final Comparator<Action> actionComparator;

  public Collator(Decider... deciders) {
    super(evalIntervalSeconds, collatorFrequency);
    this.deciders = Arrays.asList(deciders);
    this.actionComparator = new ImpactBasedActionComparator();
    this.impactAssessor = new ImpactAssessor();
  }

  /**
   * Constructor used for unit testing purposes only.
   *
   * @param impactAssessor   the impact assessor.
   * @param actionComparator comparator for sorting actions.
   * @param deciders         The participating deciders.
   */
  @VisibleForTesting
  public Collator(final ImpactAssessor impactAssessor, final Comparator<Action> actionComparator,
      Decider... deciders) {
    super(evalIntervalSeconds, collatorFrequency);
    this.deciders = Arrays.asList(deciders);
    this.actionComparator = actionComparator;
    this.impactAssessor = impactAssessor;
  }

  @Override
  public String name() {
    return NAME;
  }

  /**
   * Process all the actions proposed by the deciders and prune them based on their impact vectors.
   *
   * @return A {@link Decision} instance that contains the list of polarized actions.
   */
  @Override
  public Decision operate() {
    Decision finalDecision = new Decision(System.currentTimeMillis(), NAME);
    List<Action> allowedActions = new ArrayList<>();

    // First get all the actions proposed by the deciders and assess the overall impact all
    // actions combined have on all the affected nodes.

    List<Action> allActions = getProposedActions();
    Map<NodeKey, ImpactAssessment> overallImpactAssessment =
        impactAssessor.assessOverallImpact(allActions);

    // We need to identify and prune conflicting actions based on the overall impact. In order to
    // do that, we re-assess each of the proposed actions with the overall impact assessment in
    // mind. In each such assessment, we ensure the impact of an action aligns with the instance's
    // current pressure heading(increasing/decreasing). Actions that don't align are pruned and
    // their effects on the overall impact are undone. As the order in which we reassess matters
    // as to what actions get picked and what don't, we sort the list of actions based on a
    // simple heuristic where actions that reduce pressure the most are re-assessed later
    // thereby decreasing the chance of them getting pruned because of another action.

    allActions.sort(actionComparator);
    allActions.forEach(action -> {
      if (impactAssessor.isImpactAligned(action, overallImpactAssessment)) {
        allowedActions.add(action);
      } else {
        impactAssessor.undoActionImpactOnOverallAssessment(action, overallImpactAssessment);
      }
    });

    finalDecision.addAllActions(allowedActions);
    return finalDecision;
  }

  /**
   * Combines all actions proposed by the deciders into a list.
   *
   * @return A list of actions.
   */
  @NonNull
  private List<Action> getProposedActions() {
    final List<Action> proposedActions = new ArrayList<>();
    if (deciders != null) {
      for (final Decider decider : deciders) {
        List<Decision> decisions = decider.getFlowUnits();
        decisions.forEach(decision -> {
          if (!decision.getActions().isEmpty()) {
            proposedActions.addAll(decision.getActions());
          }
        });
      }
    }
    return proposedActions;
  }

  /**
   * A comparator for actions to sort them based on their impact from least pressure decreasing
   * to most.
   */
  @VisibleForTesting
  static final class ImpactBasedActionComparator implements Comparator<Action>, Serializable {

    @Override
    public int compare(Action action1, Action action2) {
      int numberOfPressureReductions1 = getImpactedDimensionCount(action1, Impact.DECREASES_PRESSURE);
      int numberOfPressureReductions2 = getImpactedDimensionCount(action2, Impact.DECREASES_PRESSURE);

      if (numberOfPressureReductions1 != numberOfPressureReductions2) {
        return numberOfPressureReductions1 - numberOfPressureReductions2;
      }

      int numberOfPressureIncreases1 = getImpactedDimensionCount(action1, Impact.INCREASES_PRESSURE);
      int numberOfPressureIncreases2 = getImpactedDimensionCount(action2, Impact.INCREASES_PRESSURE);

      if (numberOfPressureIncreases1 != numberOfPressureIncreases2) {
        return numberOfPressureIncreases2 - numberOfPressureIncreases1;
      }

      return 0;
    }

    private int getImpactedDimensionCount(final Action action, Impact requiredImpact) {
      int count = 0;
      for (ImpactVector impactVector : action.impact().values()) {
        for (Impact impact : impactVector.getImpact().values()) {
          if (impact.equals(requiredImpact)) {
            count++;
          }
        }
      }
      return count;
    }
  }
}
