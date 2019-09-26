package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Gatherable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Operable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class NodeOperations {
  private static final Logger LOG = LogManager.getLogger(NodeOperations.class);

  static FlowUnit handleMetric(OperationArgWrapper args) {
    return ((Gatherable) args.getNode()).gather(args.getQueryable());
  }

  static FlowUnit handleEvaluate(OperationArgWrapper args) {
    return ((Operable) args.getNode()).operate(args.getUpstreamDependencyMap());
  }

  static FlowUnit handleRca(OperationArgWrapper args) {
    FlowUnit flowUnit = handleEvaluate(args);
    if (!flowUnit.isEmpty() && !flowUnit.getData().isEmpty()) {
      args.getPersistable().write(args.getNode(), flowUnit);
    }
    return flowUnit;
  }

  // This is the abstraction for when the data arrives on the wire from a remote dependency.
  static FlowUnit readFromWire(OperationArgWrapper args) {
    return args.getWireHopper().readFromWire(args.getNode().name());
  }
}
