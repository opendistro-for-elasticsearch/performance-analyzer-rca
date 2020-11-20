package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.GCInfoCollector.GCInfo;
import org.junit.Assert;
import org.junit.Before;

public class GCInfoCollectorTest extends AbstractCollectorTest {
  @Before
  public void setup() {
    setUut(new GCInfoCollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception {
    GCInfo info = mapper.readValue(metric, GCInfo.class);
    // TODO implement further validation of the MetricStatus
    Assert.assertFalse(info.getCollectorName().isEmpty());
    Assert.assertFalse(info.getMemoryPool().isEmpty());
  }
}
