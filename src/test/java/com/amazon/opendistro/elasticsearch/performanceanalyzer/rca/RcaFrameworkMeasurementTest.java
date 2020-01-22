package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import java.lang.reflect.InvocationTargetException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaFrameworkMeasurementTest {
  @Before
  public void cleanup() {
    RcaTestFrameworkSetup.cleanUpLogs();
  }

  @Test
  public void metricEmission() {
    try {
      AnalysisGraph graph =
          RcaUtil.getAnalysisGraph(
              "com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.FaultyAnalysisGraph");
      RcaTestFrameworkSetup setup = new RcaTestFrameworkSetup(graph);
      while (!PerformanceAnalyzerApp.RCA_STATS_REPORTER.isMeasurementCollected(
          ExceptionsAndErrors.EXCEPTION_IN_OPERATE)) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      setup.getStatsCollector().collectMetrics(0);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
