package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Master_Task_Queue_Time extends Metric {
  public Master_Task_Queue_Time(long evaluationIntervalSeconds) {
    super(AllMetrics.MasterMetricValues.MASTER_TASK_QUEUE_TIME.name(), evaluationIntervalSeconds);
  }
}
