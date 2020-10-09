package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DevicePartitionValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Partition_FreeSpace extends Metric {

  public static final String NAME = DevicePartitionValue.FREE_SPACE.toString();

  public Partition_FreeSpace(long evalIntervalInSeconds) {
    super(NAME, evalIntervalInSeconds);
  }
}
