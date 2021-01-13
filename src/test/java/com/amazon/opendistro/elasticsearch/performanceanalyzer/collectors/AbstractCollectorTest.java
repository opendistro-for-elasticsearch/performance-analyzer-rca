package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.Test;

/**
 * A base class that others can extend to validate their Metrics Collectors behavior
 */
public abstract class AbstractCollectorTest {
  protected static final ObjectMapper mapper = new ObjectMapper();
  private PerformanceAnalyzerMetricsCollector uut;

  // Implementors should call this in a setup() function to set their collector
  public void setUut(PerformanceAnalyzerMetricsCollector collector) {
    this.uut = collector;
  }

  // This is the only line that implementors need to modify
  public abstract void validateMetric(String metric) throws Exception;

  @Test
  public void validateMetrics() throws Exception {
    uut.collectMetrics(Instant.now().toEpochMilli());
    String metricString = uut.getValue().toString();
    // chop off current time json
    int end = metricString.indexOf(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
    metricString = metricString.substring(end + 1);
    while (!metricString.isEmpty()) {
      end = metricString.indexOf(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
      if (end == -1) {
        break;
      }
      String metric = metricString.substring(0, end);
      validateMetric(metric);
      metricString = metricString.substring(end + 1);
    }
  }
}
