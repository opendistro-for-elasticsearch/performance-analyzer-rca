package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class ThreadPool_QueueCapacity extends Metric {
  public ThreadPool_QueueCapacity() {
    super(AllMetrics.ThreadPoolValue.THREADPOOL_QUEUE_CAPACITY.name(), 5);
  }
}
