package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MetricTestHelper extends Metric {
  private static final String METRIC_TEST_HELPER_NAME = "Metric_test_Helper";

  public MetricTestHelper(long evaluationIntervalSeconds) {
    super(METRIC_TEST_HELPER_NAME, evaluationIntervalSeconds);
  }

  public void createTestFlowUnits(final List<String> columnName, final List<String> row) {
    this.flowUnits = Collections.singletonList(new MetricFlowUnit(0, Arrays.asList(columnName, row)));
  }
}
