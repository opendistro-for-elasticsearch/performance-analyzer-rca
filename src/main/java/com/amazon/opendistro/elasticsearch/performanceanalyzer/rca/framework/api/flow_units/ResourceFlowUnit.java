package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.StringList;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import java.util.List;

public class ResourceFlowUnit extends GenericFlowUnit {
  private ResourceContext resourceContext = null;

  public ResourceFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public ResourceFlowUnit(long timeStamp, ResourceContext context) {
    super(timeStamp);
    this.resourceContext = context;
  }

  public ResourceFlowUnit(long timeStamp, List<List<String>> data, ResourceContext context) {
    super(timeStamp, data);
    this.resourceContext = context;
  }

  public ResourceContext getResourceContext() {
    return this.resourceContext;
  }

  // Call generic() only if you want to generate a empty flowunit
  public static ResourceFlowUnit generic() {
    return new ResourceFlowUnit(System.currentTimeMillis());
  }

  public FlowUnitMessage buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = FlowUnitMessage.newBuilder();
    messageBuilder.setGraphNode(graphNode);
    messageBuilder.setEsNode(esNode);

    if (!this.isEmpty()) {
      for (List<String> value : this.getData()) {
        messageBuilder.addValues(StringList.newBuilder().addAllValues(value).build());
      }
    }

    messageBuilder.setTimestamp(System.currentTimeMillis());
    if (resourceContext != null) {
      messageBuilder.setResourceContext(resourceContext.buildContextMessage());
    }
    return messageBuilder.build();
  }

  public static ResourceFlowUnit buildFlowUnitFromWrapper(final FlowUnitWrapper value) {
    if (value.hasData()) {
      return new ResourceFlowUnit(
          value.getTimeStamp(), value.getData(), value.getResourceContext());
    } else {
      return new ResourceFlowUnit(value.getTimeStamp(), value.getResourceContext());
    }
  }

  @Override
  public String toString() {
    return String.format("%d: %s :: %s", this.getTimeStamp(), this.getData(), resourceContext);
  }
}
