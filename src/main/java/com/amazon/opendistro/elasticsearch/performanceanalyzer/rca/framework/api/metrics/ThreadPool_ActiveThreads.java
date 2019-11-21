package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class ThreadPool_ActiveThreads extends Metric {
  public ThreadPool_ActiveThreads(long evaluationIntervalSeconds) {
    super(AllMetrics.ThreadPoolValue.THREADPOOL_ACTIVE_THREADS.name(), evaluationIntervalSeconds);
  }
}
