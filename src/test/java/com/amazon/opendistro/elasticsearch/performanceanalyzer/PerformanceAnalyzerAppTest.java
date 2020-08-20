package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import org.junit.Assert;
import org.junit.Test;

public class PerformanceAnalyzerAppTest {

  @Test
  public void testMain() {
    PerformanceAnalyzerApp.main(new String[0]);
    Assert.assertFalse(ConfigStatus.INSTANCE.haveValidConfig());
  }
}