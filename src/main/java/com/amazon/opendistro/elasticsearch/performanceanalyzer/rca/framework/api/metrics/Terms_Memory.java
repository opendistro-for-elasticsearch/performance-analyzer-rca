package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Terms_Memory extends Metric {
  public Terms_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.TERMS_MEMORY.name(), evaluationIntervalSeconds);
  }
}
