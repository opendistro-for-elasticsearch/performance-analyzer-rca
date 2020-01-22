package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.emitters;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaTestFrameworkSetup;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.sampled.JvmMeasurements;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class PeriodicSamplersTest {
  @Before
  public void cleanup() {
    RcaTestFrameworkSetup.cleanUpLogs();
  }

  class TestStatsCollector extends StatsCollector {
    TestStatsCollector() {
      super("stats", 1, new HashMap<>());
    }
  }

  @Test
  public void emitters() {
    TestStatsCollector tsc = new TestStatsCollector();

    PeriodicSamplers periodicSamplers =
        new PeriodicSamplers(PerformanceAnalyzerApp.SYSTEM_RESOURCE_SAMPLER);
    periodicSamplers.run();
    for (MeasurementSet measurementSet : JvmMeasurements.values()) {
      Assert.assertTrue(
          PerformanceAnalyzerApp.SYSTEM_RESOURCE_SAMPLER.isMeasurementObserved(measurementSet));
    }
    tsc.collectMetrics(0);
    RcaTestFrameworkSetup.printStatsLogs();
  }
}
