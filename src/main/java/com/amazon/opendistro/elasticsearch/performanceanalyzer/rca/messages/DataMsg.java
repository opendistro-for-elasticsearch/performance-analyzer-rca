package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import java.util.List;

public class DataMsg {
  String sourceNode;
  List<String> destinationNodes;
  FlowUnit flowUnit;

  public DataMsg(String sourceNode, List<String> destinationNode, FlowUnit flowUnit) {
    this.sourceNode = sourceNode;
    this.destinationNodes = destinationNode;
    this.flowUnit = flowUnit;
  }

  public String getSourceNode() {
    return sourceNode;
  }

  public List<String> getDestinationNode() {
    return destinationNodes;
  }

  public FlowUnit getFlowUnit() {
    return flowUnit;
  }

  @Override
  public String toString() {
    return String.format("Data::from: '%s', to: %s", sourceNode, destinationNodes);
  }
}
