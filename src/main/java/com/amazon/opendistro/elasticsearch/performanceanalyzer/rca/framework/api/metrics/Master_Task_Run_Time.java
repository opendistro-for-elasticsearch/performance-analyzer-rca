package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Master_Task_Run_Time extends Metric {
  public Master_Task_Run_Time(long evaluationIntervalSeconds) {
    super(AllMetrics.MasterMetricDimensions.MASTER_TASK_RUN_TIME.name(), evaluationIntervalSeconds);
  }
}
