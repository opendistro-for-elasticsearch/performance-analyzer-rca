package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;

import java.util.ArrayList;
import java.util.List;

public class Decision extends GenericFlowUnit {

  private List<Action> actions = new ArrayList<>();
  private String decider;

  public Decision(long timeStamp, String deciderName) {
    super(timeStamp);
    setDecider(deciderName);
  }

  public void addAction(Action action) {
    if (action != null) {
      actions.add(action);
    }
  }

  public void addAllActions(List<Action> actions) {
    this.actions.addAll(actions);
  }

  public List<Action> getActions() {
    return actions;
  }

  public String getDecider() {
    return decider;
  }

  public void setDecider(String decider) {
    this.decider = decider;
  }

  @Override
  public boolean isEmpty() {
    return actions.isEmpty();
  }

  @Override
  public FlowUnitMessage buildFlowUnitMessage(String graphNode, String esNode) {
    // All deciders run on the master node, (in initial versions), so we dont expect Decisions
    // to be passed over wire.
    throw new IllegalStateException(
        this.getClass().getSimpleName() + " not expected to be passed " + "over the wire.");
  }
}
