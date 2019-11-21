package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlowUnitWrapper {
  private long timeStamp;
  private boolean empty;
  private List<List<String>> data;
  private ResourceContext resourceContext;

  FlowUnitWrapper(long timeStamp) {
    this.timeStamp = timeStamp;
    this.empty = true;
    this.resourceContext = null;
  }

  public void setData(List<List<String>> data) {
    this.empty = false;
    this.data = data;
  }

  public void setResourceContext(ResourceContext resourceContext) {
    this.resourceContext = resourceContext;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public long getTimeStamp() {
    return this.timeStamp;
  }

  public boolean hasResourceContext() {
    return resourceContext != null;
  }

  public boolean hasData() {
    return empty == false;
  }

  public List<List<String>> getData() {
    return this.data;
  }

  public ResourceContext getResourceContext() {
    return this.resourceContext;
  }

  public static FlowUnitWrapper buildFlowUnitWrapperFromMessage(final FlowUnitMessage value) {
    FlowUnitWrapper flowUnitWrapper = new FlowUnitWrapper(value.getTimestamp());
    if (value.getValuesCount() > 0) {
      flowUnitWrapper.setData(
          value.getValuesList().stream()
              .map(
                  i ->
                      IntStream.range(0, i.getValuesCount())
                          .mapToObj(i::getValues)
                          .collect(Collectors.toList()))
              .collect(Collectors.toList()));
    }
    if (value.hasResourceContext()) {
      flowUnitWrapper.setResourceContext(
          ResourceContext.buildResourceContextFromMessage(value.getResourceContext()));
    }
    return flowUnitWrapper;
  }
}
