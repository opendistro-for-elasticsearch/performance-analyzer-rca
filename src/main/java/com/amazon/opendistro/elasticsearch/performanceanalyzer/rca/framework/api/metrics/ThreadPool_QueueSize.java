package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class ThreadPool_QueueSize extends Metric {
  public ThreadPool_QueueSize(long evaluationIntervalSeconds) {
    super(AllMetrics.ThreadPoolValue.THREADPOOL_QUEUE_SIZE.name(), evaluationIntervalSeconds);
  }
}
