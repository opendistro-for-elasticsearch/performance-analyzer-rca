package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.underutilization;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeUnderUtilizedRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {

  private static final Logger LOG = LogManager.getLogger(NodeUnderUtilizedRca.class);
  private static final long EVAL_INTERVAL_IN_S = 5;

  private final Rca<ResourceFlowUnit<HotResourceSummary>> cpuUnderUtilizedRca;
  private final Rca<ResourceFlowUnit<HotResourceSummary>> diskUnderUtilizedRca;

  public NodeUnderUtilizedRca(final Rca<ResourceFlowUnit<HotResourceSummary>> cpuUnderUtilizedRca,
      final Rca<ResourceFlowUnit<HotResourceSummary>> diskUnderUtilizedRca) {
    super(EVAL_INTERVAL_IN_S);
    this.cpuUnderUtilizedRca = cpuUnderUtilizedRca;
    this.diskUnderUtilizedRca = diskUnderUtilizedRca;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    List<FlowUnitMessage> flowUnitMessages = args.getWireHopper().readFromWire(args.getNode());

    setFlowUnits(flowUnitMessages.stream()
                                 .map((Function<FlowUnitMessage, ResourceFlowUnit<HotNodeSummary>>)
                                     ResourceFlowUnit::buildFlowUnitFromWrapper)
                                 .collect(Collectors.toList()));
  }

  @Override
  public ResourceFlowUnit<HotNodeSummary> operate() {
    long currTime = System.currentTimeMillis();
    List<ResourceFlowUnit<HotResourceSummary>> cpuUnderUtilizedFlowUnits = cpuUnderUtilizedRca
        .getFlowUnits();
    List<ResourceFlowUnit<HotResourceSummary>> diskUnderUtilizedFlowUnits = diskUnderUtilizedRca
        .getFlowUnits();

    // only one flow unit is expected for both the RCAs as they all have the same evaluation
    // frequency.

    if (cpuUnderUtilizedFlowUnits.size() != 1 || diskUnderUtilizedFlowUnits.size() != 1) {
      LOG.warn("Was expecting both CPUUnderUtilizedRca and DiskUnderUtilizedRca to have exactly "
          + "one flowunit. Found: " + cpuUnderUtilizedFlowUnits.size() + ", and "
          + diskUnderUtilizedFlowUnits.size() + " respectively");
      return new ResourceFlowUnit<>(currTime);
    }

    final ResourceFlowUnit<HotResourceSummary> cpuUnderUtilizedFlowUnit =
        cpuUnderUtilizedFlowUnits.get(0);
    final ResourceFlowUnit<HotResourceSummary> diskUnderUtilizedFlowUnit =
        diskUnderUtilizedFlowUnits.get(0);

    if (!cpuUnderUtilizedFlowUnit.isEmpty() && !diskUnderUtilizedFlowUnit.isEmpty()) {
      boolean isCpuUnderUtilized = cpuUnderUtilizedFlowUnit.getResourceContext().isUnderUtilized();
      boolean isDiskUnderUtilized = diskUnderUtilizedFlowUnit.getResourceContext().isUnderUtilized();

      if (isCpuUnderUtilized && isDiskUnderUtilized) {
        InstanceDetails instanceDetails = getAppContext().getMyInstanceDetails();
        HotNodeSummary summary = new HotNodeSummary(instanceDetails.getInstanceId(),
            instanceDetails.getInstanceIp());
        summary.appendNestedSummary(cpuUnderUtilizedFlowUnit.getSummary());
        summary.appendNestedSummary(diskUnderUtilizedFlowUnit.getSummary());

        ResourceContext context = new ResourceContext(State.UNDERUTILIZED);
        return new ResourceFlowUnit<>(currTime, context, summary);
      }
    }

    return new ResourceFlowUnit<>(currTime);
  }
}
