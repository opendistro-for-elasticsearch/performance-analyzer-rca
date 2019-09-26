package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Sched_CtxRate extends Metric {
  public Sched_CtxRate(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.SCHED_CTX_RATE.name(), evaluationIntervalSeconds);
  }
}
