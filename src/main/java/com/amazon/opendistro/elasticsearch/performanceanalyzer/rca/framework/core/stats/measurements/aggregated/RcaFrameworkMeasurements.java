package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import java.util.Arrays;
import java.util.List;

public enum RcaFrameworkMeasurements implements AggregateMeasurements {
  /** Time taken per run of the RCA graph */
  GRAPH_EXECUTION_TIME(
      "FullGraphExecutionTime",
      "micros",
      Arrays.asList(Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT));


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
