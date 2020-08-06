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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;

public class SingleNodeImpactActionGrouper implements ActionGrouper {

  /**
   * Groups actions by instance they impact. This grouping method considers only those actions that
   * impact a single node as valid candidates for grouping and as a result it filters out actions
   * that impact multiple nodes.
   *
   * <p>Any action that impacts more than one node will need a more involved handling logic and
   * this method is not to be used for grouping such actions.</p>
   *
   * @param actions The list of actions that need to be grouped.
   * @return A map of actions grouped by instance they impact.
   */
  @Override
  @NonNull
  public Map<NodeKey, List<Action>> groupByInstanceId(@NonNull List<Action> actions) {
    final Map<NodeKey, List<Action>> actionsByNodeId = new HashMap<>();
    actions.stream()
           .filter(action -> action.impactedNodes().size() == 1)
           .forEach(action -> actionsByNodeId.computeIfAbsent(action.impactedNodes()
                                                                    .get(0), k -> new ArrayList<>())
                                             .add(action));
    return actionsByNodeId;
  }
}
