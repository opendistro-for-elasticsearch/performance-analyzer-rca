package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DevicePartitionValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Partition_TotalSpace extends Metric {

  public static final String NAME = DevicePartitionValue.TOTAL_SPACE.toString();

  public Partition_TotalSpace(long evalIntervalInSeconds) {
    super(NAME, evalIntervalInSeconds);
  }
}
