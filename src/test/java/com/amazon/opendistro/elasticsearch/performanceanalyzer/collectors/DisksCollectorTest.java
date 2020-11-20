package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import org.junit.Assert;
import org.junit.Before;

public class DisksCollectorTest extends AbstractCollectorTest {
  @Before
  public void setup() {
    setUut(new DisksCollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception{
    DiskMetrics diskMetrics = mapper.readValue(metric, DiskMetrics.class);
    // TODO implement further validation of the MetricStatus
    Assert.assertFalse(diskMetrics.getName().isEmpty());
    Assert.assertTrue(diskMetrics.getUtilization() >= 0 && diskMetrics.getUtilization() <= 1);
    Assert.assertTrue(diskMetrics.getAwait() >= 0);
    Assert.assertTrue(diskMetrics.getServiceRate() >= 0);
  }
}
