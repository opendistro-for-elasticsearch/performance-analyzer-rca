package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.resource;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;

public class HotResourceFlowUnit extends ResourceFlowUnit {
  private final HotResourceSummary hotResourceSummary;

  public HotResourceFlowUnit(long timeStamp, ResourceContext context,
      HotResourceSummary hotResourceSummary, boolean persistSummary) {
    super(timeStamp, context, hotResourceSummary, persistSummary);
    this.hotResourceSummary = hotResourceSummary;
  }

  public HotResourceFlowUnit(long timeStamp) {
    super(timeStamp);
    hotResourceSummary = null;
  }

  public HotResourceSummary getHotResourceSummary() {
    return hotResourceSummary;
  }

  @Override
  public FlowUnitMessage.Builder buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = super.buildFlowUnitMessage(graphNode, esNode);

    if (hotResourceSummary != null) {
      hotResourceSummary.buildSummaryMessageAndAddToFlowUnit(messageBuilder);
    }
    return messageBuilder;
  }
}
