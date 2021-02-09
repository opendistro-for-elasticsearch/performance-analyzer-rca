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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * ImpactAssessor is responsible for assessing the impact of various actions on the nodes and
 * determining if an action is currently aligned in the same direction as the node's current
 * pressure heading in the presence of all proposed actions.
 */
public class ImpactAssessor {

  private static final Logger LOG = LogManager.getLogger(ImpactAssessor.class);

  /**
   * Combines the pressure characteristics of the given list of actions into an overall impact
   * assessment per node.
   *
   * @param actions The list of actions whose for which the impact need to assessed.
   * @return A map of instance to its overall impact on the instance based on the provided list of
   *         actions.
   */
  public @NonNull Map<NodeKey, ImpactAssessment> assessOverallImpact(
      @NonNull final List<Action> actions) {
    Map<NodeKey, ImpactAssessment> overallImpactAssessment = new HashMap<>();
    actions.forEach(action -> {
      Map<NodeKey, ImpactVector> impactMap = action.impact();
      impactMap.forEach((nodeKey, impactVector) -> overallImpactAssessment.computeIfAbsent(nodeKey,
          ImpactAssessment::new).addActionImpact(action.name(), impactVector));
    });

    return overallImpactAssessment;
  }

  /**
   * Checks if the impact of a given action aligns with the overall proposed impact for a node. An
   * action is classified as 'impact aligning' only if all the impacted nodes in the action align
   * with their proposed pressure heading.
   *
   * @param action                  the action whose impact needs to be checked for alignment.
   * @param overallImpactAssessment The impact assessment that provides the pressure heading for the
   *                                nodes.
   * @return true if all impacted nodes are in alignment.
   */
  public boolean isImpactAligned(@NonNull final Action action,
      @NonNull final Map<NodeKey, ImpactAssessment> overallImpactAssessment) {

    boolean isAligned = true;

    for (final NodeKey nodeKey : action.impactedNodes()) {
      if (!overallImpactAssessment.containsKey(nodeKey)) {
        LOG.error("Overall impact assessment does not a node key: {} for which an impacting action "
            + "exists.", nodeKey);
        return false;
      }

      final ImpactAssessment nodeImpactAssessment = overallImpactAssessment.get(nodeKey);

      isAligned = isAligned && nodeImpactAssessment.checkAlignmentAcrossDimensions(action.name(),
          action.impact().get(nodeKey));
    }

    return isAligned;
  }

  public void undoActionImpactOnOverallAssessment(@NonNull final Action action,
      @NonNull final Map<NodeKey, ImpactAssessment> overallImpactAssessment) {
    for (final NodeKey nodeKey : action.impactedNodes()) {
      if (!overallImpactAssessment.containsKey(nodeKey)) {
        LOG.error("Overall impact assessment does not a node key: {} for which an impacting action "
            + "exists.", nodeKey);
        return;
      }

      final ImpactAssessment nodeImpactAssessment = overallImpactAssessment.get(nodeKey);
      nodeImpactAssessment.removeActionImpact(action.name(), action.impact().get(nodeKey));
    }
  }
}
