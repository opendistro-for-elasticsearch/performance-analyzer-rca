package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Stats {
  private static Stats instance = null;
  private long totalNodesCount;
  private long leafNodesCount;
  private long leavesAddedToAnalysisFlowField;
  private Map<Integer, ConnectedComponent> graphs;

  private Stats() {
    this.totalNodesCount = 0;
    this.leafNodesCount = 0;
    this.leavesAddedToAnalysisFlowField = 0;
    this.graphs = new HashMap<>();
  }

  public static Stats getInstance() {
    if (instance == null) {
      instance = new Stats();
    }
    return instance;
  }

  public static void clear() {
    instance = null;
  }

  public void incrementLeavesAddedToAnalysisFlowField() {
    ++this.leavesAddedToAnalysisFlowField;
  }

  public long getTotalNodesCount() {
    return totalNodesCount;
  }

  public long getLeafNodesCount() {
    return leafNodesCount;
  }

  public long getLeavesAddedToAnalysisFlowField() {
    return leavesAddedToAnalysisFlowField;
  }

  void incrementTotalNodesCount() {
    ++this.totalNodesCount;
  }

  void incrementLeafNodesCount() {
    ++this.leafNodesCount;
    incrementTotalNodesCount();
  }

  public int getGraphsCount() {
    return graphs.size();
  }

  public void addNewGraph(ConnectedComponent connectedComponent) {
    this.graphs.put(connectedComponent.getGraphId(), connectedComponent);
  }

  void removeGraph(int graphId) {
    this.graphs.remove(graphId);
  }

  public ConnectedComponent getGraphById(int graphId) {
    return this.graphs.get(graphId);
  }

  public List<ConnectedComponent> getConnectedComponents() {
    return new ArrayList<>(graphs.values());
  }

  public void reset() {
    totalNodesCount = 0;
    leafNodesCount = 0;
    leavesAddedToAnalysisFlowField = 0;
    graphs = null;
  }
}
