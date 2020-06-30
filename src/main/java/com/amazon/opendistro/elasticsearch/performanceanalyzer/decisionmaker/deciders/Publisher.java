package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Publisher extends NonLeafNode<GenericFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(Publisher.class);

  private Collator collator;
  private boolean isMuted = false;

  public Publisher(int evalIntervalSeconds, Collator collator) {
    super(0, evalIntervalSeconds);
    this.collator = collator;
  }

  @Override
  public GenericFlowUnit operate() {
    // TODO: Pass through implementation, need to add dampening, cool-off, action flip-flop
    // avoidance, state persistence etc.

    Decision decision = collator.getFlowUnits().get(0);
    for (Action action : decision.getActions()) {
      LOG.info("Executing action: [{}]", action.name());
      action.execute();
    }

    return new GenericFlowUnit(System.currentTimeMillis()) {
      @Override
      public FlowUnitMessage buildFlowUnitMessage(String graphNode, String esNode) {
        throw new IllegalStateException(
            this.getClass().getSimpleName() + " not expected to be passed over wire");
      }
    };
  }

  /* Publisher does not have downstream nodes and does not emit flow units
   */

  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void handleNodeMuted() {
    assert true;
  }
}
