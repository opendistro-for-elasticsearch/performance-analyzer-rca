package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.GCInfoCollector.GCInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;

public class GCInfoCollectorTest extends AbstractCollectorTest {
  private static final ObjectMapper mapper = new ObjectMapper();

  @Before
  public void setup() {
    setUut(new GCInfoCollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception{
    GCInfo info = mapper.readValue(metric, GCInfo.class);
    // TODO implement validation of the MetricStatus
  }
}
