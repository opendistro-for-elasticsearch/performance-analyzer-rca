/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GraphNodeOperations {
  private static final Logger LOG = LogManager.getLogger(GraphNodeOperations.class);

  static void readFromLocal(FlowUnitOperationArgWrapper args) {
    if (Stats.getInstance().isNodeMuted(args.getNode().name())) {
      args.getNode().handleNodeMuted();
      return;
    }
    args.getNode().generateFlowUnitListFromLocal(args);
    args.getNode().persistFlowUnit(args);
    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
        RcaGraphMetrics.NUM_NODES_EXECUTED_LOCALLY, "", 1);
  }

  // This is the abstraction for when the data arrives on the wire from a remote dependency.
  static void readFromWire(FlowUnitOperationArgWrapper args) {
    // flowUnits.forEach(i -> LOG.info("rca: Read from wire: {}", i));
    args.getNode().generateFlowUnitListFromWire(args);
    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
        RcaGraphMetrics.NUM_NODES_EXECUTED_REMOTELY, "", 1);
  }
}
