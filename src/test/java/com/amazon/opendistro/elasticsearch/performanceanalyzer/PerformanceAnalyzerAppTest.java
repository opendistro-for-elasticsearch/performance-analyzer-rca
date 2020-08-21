package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import org.junit.Assert;
import org.junit.Test;

public class PerformanceAnalyzerAppTest {

  @Test
  public void testMain() {
    PerformanceAnalyzerApp.main(new String[0]);
    Assert.assertFalse(ConfigStatus.INSTANCE.haveValidConfig());
    Assert.assertEquals(StatsCollector.instance().getCounters().get("ReaderThreadStopped").get(), 1);
  }
}