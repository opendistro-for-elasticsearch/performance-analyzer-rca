/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LargeHeapClusterRca extends Rca<ResourceFlowUnit<HotClusterSummary>> {

  private static final long EVAL_INTERVAL_IN_S = 5;

  private final Rca<ResourceFlowUnit<HotNodeSummary>> oldGenContendedRca;

  public LargeHeapClusterRca(final Rca<ResourceFlowUnit<HotNodeSummary>> oldGenContendedRca) {
    super(EVAL_INTERVAL_IN_S);
    this.oldGenContendedRca = oldGenContendedRca;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new UnsupportedOperationException("generateFlowUnitListFromWire is not supported on the"
        + " node-local RCA: " + args.getNode().name());
  }

  @Override
  public ResourceFlowUnit<HotClusterSummary> operate() {
    List<ResourceFlowUnit<HotNodeSummary>> oldGenContendedFlowUnits = oldGenContendedRca
        .getFlowUnits();
    List<HotNodeSummary> unhealthyNodeSummaries = new ArrayList<>();
    long currTime = System.currentTimeMillis();
    for (ResourceFlowUnit<HotNodeSummary> flowUnit : oldGenContendedFlowUnits) {
      if (flowUnit.isEmpty()) {
        continue;
      }

      if (flowUnit.getResourceContext().isUnhealthy()) {
        unhealthyNodeSummaries.add(flowUnit.getSummary());
      }
    }

    if (unhealthyNodeSummaries.isEmpty()) {
      return new ResourceFlowUnit<>(currTime);
    }

    final HotClusterSummary summary =
        new HotClusterSummary(getAppContext().getAllClusterInstances().size(),
            unhealthyNodeSummaries.stream().map(HotNodeSummary::getNodeID).collect(
                Collectors.toSet()).size());
    for (HotNodeSummary hotNodeSummary : unhealthyNodeSummaries) {
      summary.appendNestedSummary(hotNodeSummary);
    }

    final ResourceContext context = new ResourceContext(State.CONTENDED);

    return new ResourceFlowUnit<>(currTime, context, summary);
  }
}
