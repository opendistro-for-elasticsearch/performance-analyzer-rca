package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class ThreadPool_RejectedReqs extends Metric {
  public ThreadPool_RejectedReqs(long evaluationIntervalSeconds) {
    super(AllMetrics.ThreadPoolValue.THREADPOOL_REJECTED_REQS.name(), evaluationIntervalSeconds);
  }
}
