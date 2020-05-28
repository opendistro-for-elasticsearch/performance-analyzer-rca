package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.resource;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;

public class HotNodeFlowUnit extends ResourceFlowUnit {
  private final HotNodeSummary hotNodeSummary;

  public HotNodeFlowUnit(long timeStamp, ResourceContext context,
      HotNodeSummary hotNodeSummary, boolean persistSummary) {
    super(timeStamp, context, hotNodeSummary, persistSummary);
    this.hotNodeSummary = hotNodeSummary;
  }

  public HotNodeFlowUnit(long timeStamp) {
    super(timeStamp);
    hotNodeSummary = null;
  }

  public HotNodeSummary getHotNodeSummary() {
    return hotNodeSummary;
  }

  @Override
  public FlowUnitMessage.Builder buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = super.buildFlowUnitMessage(graphNode, esNode);

    if (hotNodeSummary != null) {
      hotNodeSummary.buildSummaryMessageAndAddToFlowUnit(messageBuilder);
    }
    return messageBuilder;
  }
}
