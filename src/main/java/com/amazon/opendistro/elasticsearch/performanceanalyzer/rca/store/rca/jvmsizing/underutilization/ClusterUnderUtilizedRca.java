package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.underutilization;

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

public class ClusterUnderUtilizedRca extends Rca<ResourceFlowUnit<HotClusterSummary>> {
  private static final long EVAL_INTERVAL_IN_S = 5;

  private final Rca<ResourceFlowUnit<HotNodeSummary>> nodeUnderUtilizedRca;

  public ClusterUnderUtilizedRca(final Rca<ResourceFlowUnit<HotNodeSummary>> nodeUnderUtilizedRca) {
    super(EVAL_INTERVAL_IN_S);
    this.nodeUnderUtilizedRca = nodeUnderUtilizedRca;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new UnsupportedOperationException("generateFlowUnitFromWire is not supported on this "
        + "node-local RCA: " + args.getNode().name());
  }

  @Override
  public ResourceFlowUnit<HotClusterSummary> operate() {
    long currTime = System.currentTimeMillis();
    List<ResourceFlowUnit<HotNodeSummary>> nodeUnderUtilizedFlowUnits = nodeUnderUtilizedRca
        .getFlowUnits();

    List<HotNodeSummary> underUtilizedNodeSummaries = new ArrayList<>();
    for (final ResourceFlowUnit<HotNodeSummary> flowUnit : nodeUnderUtilizedFlowUnits) {
      if (flowUnit.isEmpty()) {
        continue;
      }

      if (flowUnit.getResourceContext().isUnderUtilized()) {
        underUtilizedNodeSummaries.add(flowUnit.getSummary());
      }
    }

    if (underUtilizedNodeSummaries.isEmpty()) {
      return new ResourceFlowUnit<>(currTime);
    }

    final HotClusterSummary clusterSummary = new HotClusterSummary(
        getAppContext().getAllClusterInstances().size(),
        underUtilizedNodeSummaries.stream().map(HotNodeSummary::getNodeID).collect(
            Collectors.toSet()).size());

    for (final HotNodeSummary underUtilizedNodeSummary : underUtilizedNodeSummaries) {
      clusterSummary.appendNestedSummary(underUtilizedNodeSummary);
    }

    ResourceContext context = new ResourceContext(State.UNDERUTILIZED);
    return new ResourceFlowUnit<>(currTime, context, clusterSummary);
  }
}
