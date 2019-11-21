package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.SymptomContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import java.util.List;

public class SymptomFlowUnit extends GenericFlowUnit {
  private SymptomContext context = null;

  public SymptomFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public SymptomFlowUnit(long timeStamp, SymptomContext context) {
    super(timeStamp);
    this.context = context;
  }

  public SymptomFlowUnit(long timeStamp, List<List<String>> data, SymptomContext context) {
    super(timeStamp, data);
    this.context = context;
  }

  public SymptomContext getContext() {
    return this.context;
  }

  public static SymptomFlowUnit generic() {
    return new SymptomFlowUnit(System.currentTimeMillis());
  }

  public FlowUnitMessage buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = FlowUnitMessage.newBuilder();
    messageBuilder.setGraphNode(graphNode);
    messageBuilder.setEsNode(esNode);

    messageBuilder.setTimestamp(System.currentTimeMillis());

    return messageBuilder.build();
  }

  @Override
  public String toString() {
    return String.format("%d", this.getTimeStamp());
  }
}
