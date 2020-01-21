package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaFrameworkMeasurementTest {
  @Test
  public void metricEmission() throws IOException {
    try {
      AnalysisGraph graph =
          RcaUtil.getAnalysisGraph(
              "com.amazon.opendistro"
                  + ".elasticsearch"
                  + ".performanceanalyzer.rca.store"
                  + ".AnalysisGraphTest");
      RcaTestFrameworkSetup setup = new RcaTestFrameworkSetup(graph);
      //setup.cleanUpLogs();
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

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
