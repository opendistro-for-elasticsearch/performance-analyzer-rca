package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class HTTP_RequestDocs extends Metric {
  public HTTP_RequestDocs(long evaluationIntervalSeconds) {
    super(AllMetrics.HttpMetric.HTTP_REQUEST_DOCS.name(), evaluationIntervalSeconds);
  }
}
