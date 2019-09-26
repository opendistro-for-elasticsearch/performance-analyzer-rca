package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

public abstract class LeafNode extends Node implements Gatherable {
  private boolean addedToFlowField;

  public LeafNode(int level, long evaluationIntervalSeconds) {
    super(level, evaluationIntervalSeconds);
    Stats stats = Stats.getInstance();
    stats.incrementLeafNodesCount();
  }

  public boolean isAddedToFlowField() {
    return addedToFlowField;
  }

  public void setAddedToFlowField() {
    this.addedToFlowField = true;
  }
}
