/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

import java.util.List;
import java.util.Map;

public interface Action {

  /**
   * Returns true if the configured action is actionable, false otherwise.
   *
   * <p>Examples of non-actionable actions are resource configurations where limits have been
   * reached.
   */
  boolean isActionable();

  /** Time to wait since last recommendation, before suggesting this action again */
  int coolOffPeriodInSeconds();

  /**
   * Called when the action is invoked.
   *
   * <p>Specific implementation may include executing the action, or invoking downstream APIs
   */
  void execute();

  /** Returns a list of Elasticsearch nodes impacted by this action. */
  List<NodeKey> impactedNodes();

  /** Returns a map of Elasticsearch nodes to ImpactVector of this action on that node */
  Map<NodeKey, ImpactVector> impact();

  /** Returns action name */
  String name();

  /** Returns a summary for the configured action */
  String summary();
}
