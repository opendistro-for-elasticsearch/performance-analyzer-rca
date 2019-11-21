package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import java.util.List;

public class DataMsg {
  String sourceNode;
  List<String> destinationNodes;
  List<? extends GenericFlowUnit> flowUnits;

  public DataMsg(
      String sourceNode, List<String> destinationNode, List<? extends GenericFlowUnit> flowUnits) {
    this.sourceNode = sourceNode;
    this.destinationNodes = destinationNode;
    this.flowUnits = flowUnits;
  }

  public String getSourceNode() {
    return sourceNode;
  }

  public List<String> getDestinationNode() {
    return destinationNodes;
  }

  public List<? extends GenericFlowUnit> getFlowUnits() {
    return flowUnits;
  }

  @Override
  public String toString() {
    return String.format("Data::from: '%s', to: %s", sourceNode, destinationNodes);
  }
}
