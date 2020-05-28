package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.resource;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;

public class HotClusterFlowUnit extends ResourceFlowUnit {
  private final HotClusterSummary hotClusterSummary;

  public HotClusterFlowUnit(long timeStamp, ResourceContext context,
      HotClusterSummary hotClusterSummary, boolean persistSummary) {
    super(timeStamp, context, hotClusterSummary, persistSummary);
    this.hotClusterSummary = hotClusterSummary;
  }

  public HotClusterFlowUnit(long timeStamp) {
    super(timeStamp);
    hotClusterSummary = null;
  }

  @Override
  public FlowUnitMessage.Builder buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = super.buildFlowUnitMessage(graphNode, esNode);

    if (hotClusterSummary != null) {
      hotClusterSummary.buildSummaryMessageAndAddToFlowUnit(messageBuilder);
    }
    return messageBuilder;
  }
}
