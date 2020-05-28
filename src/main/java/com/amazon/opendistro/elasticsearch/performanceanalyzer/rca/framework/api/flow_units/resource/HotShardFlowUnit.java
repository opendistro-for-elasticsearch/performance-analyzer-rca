package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.resource;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotShardSummary;

public class HotShardFlowUnit extends ResourceFlowUnit {
  private final HotShardSummary hotShardSummary;

  public HotShardFlowUnit(long timeStamp, ResourceContext context,
      HotShardSummary hotShardSummary, boolean persistSummary) {
    super(timeStamp, context, hotShardSummary, persistSummary);
    this.hotShardSummary = hotShardSummary;
  }

  public HotShardFlowUnit(long timeStamp) {
    super(timeStamp);
    hotShardSummary = null;
  }

  public HotShardSummary getHotShardSummary() {
    return hotShardSummary;
  }

  @Override
  public FlowUnitMessage.Builder buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = super.buildFlowUnitMessage(graphNode, esNode);

    if (hotShardSummary != null) {
      hotShardSummary.buildSummaryMessageAndAddToFlowUnit(messageBuilder);
    }
    return messageBuilder;
  }
}
