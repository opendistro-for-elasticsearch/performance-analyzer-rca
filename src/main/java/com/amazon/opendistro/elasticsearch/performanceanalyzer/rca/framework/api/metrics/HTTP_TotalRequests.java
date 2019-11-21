package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class HTTP_TotalRequests extends Metric {
  public HTTP_TotalRequests(long evaluationIntervalSeconds) {
    super(AllMetrics.HttpMetric.HTTP_TOTAL_REQUESTS.name(), evaluationIntervalSeconds);
  }
}
