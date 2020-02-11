/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum RcaGraphMetrics implements MeasurementSet {
  /** Time taken per run of the RCA graph */
  GRAPH_EXECUTION_TIME(
      "RcaGraphExecution",
      "millis",
      Arrays.asList(
          Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

  /** Measures the time spent in the operate() method of a graph node. */
  GRAPH_NODE_OPERATE_CALL(
      "OperateCall", "millis", Arrays.asList(Statistics.MAX, Statistics.MEAN, Statistics.SUM)),

  /** Measures the time taken to call gather on metrics */
  METRIC_GATHER_CALL(
      "MetricGatherCall", "millis", Arrays.asList(Statistics.MAX, Statistics.MEAN, Statistics.SUM)),

  /** Measures the time spent in the persistence layer. */
  RCA_PERSIST_CALL(
      "RcaPersistCall", "micros", Arrays.asList(Statistics.MAX, Statistics.MEAN, Statistics.SUM)),

  NUM_GRAPH_NODES("NumGraphNodes", "count", Collections.singletonList(Statistics.SAMPLE)),

  NUM_GRAPH_NODES_MUTED(
      "NUMOfMutedGraphNodes", "count", Collections.singletonList(Statistics.SAMPLE)),

  NUM_NODES_EXECUTED_LOCALLY(
      "NodesExecutedLocally", "count", Collections.singletonList(Statistics.COUNT)),

  NUM_NODES_EXECUTED_REMOTELY(
      "NodesExecutedRemotely", "count", Collections.singletonList(Statistics.COUNT)),

  /** Measures number of bytes that was sent out as part of a protobuf message. */
  NET_BYTES_OUT("TotalRcaBytesOutSerialized", "bytes", Collections.singletonList(Statistics.SUM)),

  /** Measures number of bytes that was received as part of a protobuf message. */
  NET_BYTES_IN("TotalRcaBytesInSerialized", "bytes", Collections.singletonList(Statistics.SUM)),

  /** Number of nodes that are currently publishing flow units to downstream nodes. */
  RCA_NODES_FU_PUBLISH_COUNT("RcaFlowUnitPublishCount", "namedCount",
      Collections.singletonList(Statistics.NAMED_COUNTERS)),

  /** Number of nodes that are currently receiving flow units from upstream nodes. */
  RCA_NODES_FU_CONSUME_COUNT("RcaFlowUnitConsumeCount", "namedCount",
      Collections.singletonList(Statistics.NAMED_COUNTERS)),

  /** Number of subscription requests sent per node. */
  RCA_NODES_SUB_REQ_COUNT("RcaSubscriptionRequestCount", "namedCount",
      Collections.singletonList(Statistics.NAMED_COUNTERS)),

  /** Number of subscriptions acknowledged per node. */
  RCA_NODES_SUB_ACK_COUNT("RcaSubscriptionAckCount", "namedCount",
      Collections.singletonList(Statistics.NAMED_COUNTERS));

  /** What we want to appear as the metric name. */
  private String name;

  /**
   * The unit the measurement is in. This is not used for the statistics calculations but as an
   * information that will be dumped with the metrics.
   */
  private String unit;

  /**
   * Multiple statistics can be collected for each measurement like MAX, MIN and MEAN. This is a
   * collection of one or more such statistics.
   */
  private List<Statistics> statsList;

  RcaGraphMetrics(String name, String unit, List<Statistics> statisticList) {
    this.name = name;
    this.unit = unit;
    this.statsList = statisticList;
  }

  public String toString() {
    return new StringBuilder(name).append("-").append(unit).toString();
  }

  @Override
  public List<Statistics> getStatsList() {
    return statsList;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getUnit() {
    return unit;
  }
}
