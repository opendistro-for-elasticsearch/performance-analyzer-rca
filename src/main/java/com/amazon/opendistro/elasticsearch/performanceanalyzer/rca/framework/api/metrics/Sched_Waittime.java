package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Sched_Waittime extends Metric {
  public Sched_Waittime(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.SCHED_WAITTIME.name(), evaluationIntervalSeconds);
  }
}
