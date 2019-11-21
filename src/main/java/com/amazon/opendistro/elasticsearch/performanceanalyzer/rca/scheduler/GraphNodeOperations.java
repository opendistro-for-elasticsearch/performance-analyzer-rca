package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GraphNodeOperations {
  private static final Logger LOG = LogManager.getLogger(GraphNodeOperations.class);

  static void readFromLocal(FlowUnitOperationArgWrapper args) {
    args.getNode().generateFlowUnitListFromLocal(args);
  }

  // This is the abstraction for when the data arrives on the wire from a remote dependency.
  static void readFromWire(FlowUnitOperationArgWrapper args) {
    // flowUnits.forEach(i -> LOG.info("rca: Read from wire: {}", i));
    args.getNode().generateFlowUnitListFromWire(args);
  }
}
