/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import java.util.Collections;
import java.util.List;

public enum ExceptionsAndErrors implements MeasurementSet {
  RCA_FRAMEWORK_CRASH("RcaFrameworkCrash"),

  /**
   * These are the cases when an exception was throws in the {@code operate()} method, that each RCA
   * graph node implements.
   */
  EXCEPTION_IN_OPERATE("ExceptionInOperate", "namedCount", Statistics.NAMED_COUNTERS),

  /**
   * These are the cases when an exception was throws in the {@code compute()} method in publisher.
   */
  EXCEPTION_IN_COMPUTE("ExceptionInCompute", "namedCount", Statistics.NAMED_COUNTERS),

  /**
   * When calling the MetricsDB API throws an exception.
   */
  EXCEPTION_IN_GATHER("ExceptionInGather", "namedCount", Statistics.NAMED_COUNTERS),

  /**
   * When persisting action or flowunits, the persistable throws an exception when it is unable to write to DB.
   */
  EXCEPTION_IN_PERSIST("ExceptionInPersist", "namedCount", Statistics.NAMED_COUNTERS),

  /**
   * When the reader encounters errors accessing metricsdb files.
   */
  READER_METRICSDB_ACCESS_ERRORS("ReaderMetricsdbAccessError"),

  SHARD_STATE_COLLECTOR_ERROR("ShardStateCollectorError"),

  MASTER_THROTTLING_COLLECTOR_ERROR("MasterThrottlingMetricsCollector"),

  FAULT_DETECTION_COLLECTOR_ERROR("FaultDetectionMetricsCollector"),

  SHARD_INDEXING_PRESSURE_COLLECTOR_ERROR("ShardIndexingPressureMetricsCollector");

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
