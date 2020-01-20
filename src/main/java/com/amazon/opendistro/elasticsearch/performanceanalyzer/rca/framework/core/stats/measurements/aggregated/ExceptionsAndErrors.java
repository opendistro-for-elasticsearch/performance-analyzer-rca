package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import java.util.Collections;
import java.util.List;

public enum ExceptionsAndErrors implements AggregateMeasurements {
  RCA_FRAMEWORK_CRASH("RcaFrameworkCrash"),

  /**
   * These are the cases when an exception was throws in the {@code operate()} method, that each RCA
   * graph node implements.
   */
  EXCEPTION_IN_OPERATE("ExceptionInOperate", "namedCount", Statistics.NAMED_COUNTERS);

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

  ExceptionsAndErrors(String name) {
    this.name = name;
    this.unit = "count";
    this.statsList = Collections.singletonList(Statistics.COUNT);
  }

  ExceptionsAndErrors(String name, String unit, Statistics stats) {
    this.name = name;
    this.unit = unit;
    this.statsList = Collections.singletonList(stats);
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
