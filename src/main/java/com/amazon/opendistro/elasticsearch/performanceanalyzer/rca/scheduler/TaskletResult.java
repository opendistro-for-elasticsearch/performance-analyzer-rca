package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;


/**
 * Tasklet is a wrapper on top of a node so that a node can be executed based on its dependency
 * order using the Java Executor framework. When the callable returns, just getting the result as
 * flowUnit is not sufficient, we also need to know the node its coming from because the argument to
 * the operate method is a Map of Node.Class and the flow Unit. The TaskletResult is such a pair.
 */
public class TaskletResult {
  private Class<? extends Node> node;
  // private List<GenericFlowUnit> flowUnits;

  public Class<? extends Node> getNode() {
    return node;
  }

  /*
  public List<GenericFlowUnit> getFlowUnits() {
      return flowUnits;
  }


  public TaskletResult(Class<? extends Node> node, List<GenericFlowUnit> flowUnits) {
      this.node = node;
      this.flowUnits = flowUnits;
  }
   */

  public TaskletResult(Class<? extends Node> node) {
    this.node = node;
  }
}
