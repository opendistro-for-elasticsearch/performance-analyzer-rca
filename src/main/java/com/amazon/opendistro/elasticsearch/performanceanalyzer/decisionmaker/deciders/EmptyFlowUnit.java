package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;

public class EmptyFlowUnit extends GenericFlowUnit {

  public EmptyFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  @Override
  public FlowUnitMessage buildFlowUnitMessage(String graphNode, String esNode) {
    throw new IllegalStateException(this.getClass().getSimpleName() + " not expected to be passed over wire");
  }
}
