package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.sampled;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import java.util.Collections;
import java.util.List;

/**
 * These are the group of measurements that are sampled every so often and then they are reported.
 * They are not aggregated.
 */
public enum LivenessMeasurements implements SampledMeasurements {
  JVM_FREE_MEM_SAMPLER("JvmFreeMem", "bytes"),
  JVM_TOTAL_MEM_SAMPLER("JvmTotalMem", "bytes"),
  THREAD_COUNT("ThreadCount", "count");

  private String name;
  private String unit;

  LivenessMeasurements(String name, String unit) {
    this.name = name;
    this.unit = unit;
  }

  @Override
  public List<Statistics> getStatsList() {
    return Collections.emptyList();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  @Override
  public String toString() {
    return new StringBuilder(name).append("-").append(unit).toString();
  }
}
