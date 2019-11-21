package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Thread_Blocked_Event extends Metric {
  public Thread_Blocked_Event(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.THREAD_BLOCKED_EVENT.name(), evaluationIntervalSeconds);
  }
}
