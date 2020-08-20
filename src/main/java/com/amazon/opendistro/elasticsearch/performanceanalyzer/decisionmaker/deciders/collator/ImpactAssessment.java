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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * ImpactAssessment maintains and helps with updating impacts of actions on a node.
 */
public class ImpactAssessment {

  private static final Logger LOG = LogManager.getLogger(ImpactAssessment.class);
  private final NodeKey nodeKey;

  private final Map<Dimension, List<String>> perDimensionPressureDecreasingActions;
  private final Map<Dimension, List<String>> perDimensionPressureIncreasingActions;

  public ImpactAssessment(final NodeKey nodeKey) {
    this.nodeKey = nodeKey;
    this.perDimensionPressureDecreasingActions = new HashMap<>();
    this.perDimensionPressureIncreasingActions = new HashMap<>();
  }

  /**
   * Adds the action's impact to the current overall impact of all proposed actions for this node
   * so far.
   * @param actionName The name of the action.
   * @param impactVector The impact vector which gives a pressure heading for various resources
   *                     impacted by taking this action.
   */
  public void addActionImpact(@NonNull final String actionName,
      @NonNull final ImpactVector impactVector) {
    final Map<Dimension, Impact> impactMap = impactVector.getImpact();

    impactMap.forEach((dimension, impact) -> {
      switch (impact) {
        case INCREASES_PRESSURE:
          addActionToMap(perDimensionPressureIncreasingActions, actionName, dimension);
          break;
        case DECREASES_PRESSURE:
          addActionToMap(perDimensionPressureDecreasingActions, actionName, dimension);
          break;
        case NO_IMPACT:
          break;
        default:
          LOG.warn("Unknown impact value: {} encountered while adding action: {}'s impact",
              impact, actionName);
      }
    });
  }

  /**
   * Removes an action's impact from the current overall impact of all proposed actions for this
   * node so far.
   * @param actionName The name of the action.
   * @param impactVector The impact vector which gives a pressure heading for various resources
   *                     impacted by taking this action.
   */
  public void removeActionImpact(@NonNull final String actionName,
      @NonNull ImpactVector impactVector) {
    final Map<Dimension, Impact> impactMap = impactVector.getImpact();

    impactMap.forEach((dimension, impact) -> {
      switch (impact) {
        case INCREASES_PRESSURE:
          removeActionFromMap(perDimensionPressureIncreasingActions, actionName, dimension);
          break;
        case DECREASES_PRESSURE:
          removeActionFromMap(perDimensionPressureDecreasingActions, actionName, dimension);
          break;
        case NO_IMPACT:
          break;
        default:
          LOG.warn("Unknown impact value: {} encountered while removing action: {}'s impact",
              impact, actionName);
      }
    });
  }

  /**
   * Checks if the given impact vector aligns with the current overall impact for this node.
   * Alignment is checked when reassessing the actions where all impacts are replayed.
   *
   * @param actionName The name of the action.
   * @param impactVector The impact vector which gives a pressure heading for various resources
   *                     impacted by taking this action.
   * @return true if the impact vector aligns with the overall impact, false otherwise.
   */
  public boolean checkAlignmentAcrossDimensions(@NonNull final String actionName,
      @NonNull final ImpactVector impactVector) {
    boolean isAligned = true;

    // If this is an action that increases pressure along some dimension for this node, and the
    // overall assessment says there are actions that decrease pressure along those same
    // dimensions, then this action is not aligned with the other proposed actions where the
    // deciders are trying to reduce pressure for those dimensions.

    final Map<Dimension, Impact> impactMap = impactVector.getImpact();
    for (final Map.Entry<Dimension, Impact> entry : impactMap.entrySet()) {
      final Impact impactOnDimension = entry.getValue();
      if (isAligned && impactOnDimension.equals(Impact.INCREASES_PRESSURE)) {
        List<String> pressureDecreasingActions = perDimensionPressureDecreasingActions
            .getOrDefault(entry.getKey(), Collections.emptyList());
        isAligned = pressureDecreasingActions.isEmpty();

        if (!isAligned) {
          LOG.info("action: {}'s impact is not aligned with node: {}'s overall impact for "
                  + "dimension: {}. Found pressure decreasing actions: {}", actionName, nodeKey,
              entry.getKey(), pressureDecreasingActions);
        }
      }
    }

    return isAligned;
  }

  private void addActionToMap(@NonNull final Map<Dimension, List<String>> map,
      @NonNull final String actionName, @NonNull final Dimension dimension) {
    map.computeIfAbsent(dimension,
        dim -> new ArrayList<>()).add(actionName);
  }

  private void removeActionFromMap(@NonNull final Map<Dimension, List<String>> map,
      @NonNull final String actionName, @NonNull final Dimension dimension) {
    final List<String> actions = map.get(dimension);
    if (actions != null) {
      actions.remove(actionName);
    }
  }
}
