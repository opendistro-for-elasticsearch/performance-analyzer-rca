package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class ThreadPool_TotalThreads extends Metric {
  public ThreadPool_TotalThreads(long evaluationIntervalSeconds) {
    super(AllMetrics.ThreadPoolValue.THREADPOOL_TOTAL_THREADS.name(), evaluationIntervalSeconds);
  }
}
