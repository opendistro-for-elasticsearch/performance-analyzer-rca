package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum RcaGraphMeasurements implements AggregateMeasurements {
  /** Measures the time spent in the operate() method of a graph node. */
  GRAPH_NODE_OPERATE_CALL("OperateCall", "micros", Arrays.asList(Statistics.MAX, Statistics.MEAN)),

  /** Measures the time taken to call gather on metrics */
  METRIC_GATHER_CALL("MetricGatherCall", "micros", Arrays.asList(Statistics.MAX, Statistics.MEAN)),

  /** Measures the time spent in the persistence layer. */
  RCA_PERSIST_CALL("RcaPersistCall", "micros", Arrays.asList(Statistics.MAX, Statistics.MEAN)),

  NUM_GRAPH_NODES("NumGraphNodes", "count", Collections.singletonList(Statistics.SAMPLE)),

  NUM_NODES_EXECUTED_LOCALLY(
      "NodesExecutedLocally", "count", Collections.singletonList(Statistics.COUNT)),

  NUM_NODES_EXECUTED_REMOTELY(
      "NodesExecutedRemotely", "count", Collections.singletonList(Statistics.COUNT));

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

  RcaGraphMeasurements(String name, String unit, List<Statistics> statisticList) {
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
