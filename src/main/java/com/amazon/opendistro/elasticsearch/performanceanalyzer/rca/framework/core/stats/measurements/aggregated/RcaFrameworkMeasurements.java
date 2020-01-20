package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum RcaFrameworkMeasurements implements AggregateMeasurements {
  /** Time taken per run of the RCA graph */
  GRAPH_EXECUTION_TIME(
      "FullGraphExecutionTime",
      "micros",
      Arrays.asList(Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT)),

  /**
   * These are the flow units that are sent by the RCA framework for the remote nodes. Not all of
   * them may or may not make it over the wire.
   */
  FLOW_UNITS_SENT_TO_WIRE_HOPPER(
      "FlowUnitsToWireHopper", "count", Collections.singletonList(Statistics.COUNT)),

  /**
   * This measures how many flow units are sent over the wire. If there are multiple subscribers,
   * then the flow unit will be copied to each one of them. Each of the copies will be accounted for
   * in the count.
   */
  // TODO(yojs): waiting on
  // https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/59
  FLOW_UNITS_SENT_OVER_NETWORK(
      "FlowUnitsSentOverNetwork", "count", Collections.singletonList(Statistics.COUNT)),

  /**
   * The number of times the framework was stopped by the operator.
   */
  RCA_STOPPED_BY_OPERATOR(
      "RcaStoppedByOperator", "count", Collections.singletonList(Statistics.COUNT)),

  /**
   * The number of times the framework was restarted by the operator.
   */
  RCA_RESTARTED_BY_OPERATOR(
      "RcaRestartedByOperator", "count", Collections.singletonList(Statistics.COUNT)),

  /**
   * ES APIs calls are expensive and we want to keep track of how many we are making. This is a
   * named counter and therefore we can get a count per ES API.
   */
  ES_APIS_CALLED("ESApisCalled", "count", Collections.singletonList(Statistics.NAMED_COUNTERS));

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

  RcaFrameworkMeasurements(String name, String unit, List<Statistics> statisticList) {
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
