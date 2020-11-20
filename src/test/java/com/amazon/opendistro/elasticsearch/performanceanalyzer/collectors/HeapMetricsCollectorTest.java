package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector.HeapStatus;
import org.junit.Assert;
import org.junit.Before;

public class HeapMetricsCollectorTest extends AbstractCollectorTest {
  @Before
  public void setup() {
    setUut(new HeapMetricsCollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception{
    HeapStatus heapStatus = mapper.readValue(metric, HeapStatus.class);
    // TODO implement further validation of the MetricStatus
    Assert.assertFalse(heapStatus.getType().isEmpty());
    long collectionCount = heapStatus.getCollectionCount();
    Assert.assertTrue(collectionCount >= 0 || collectionCount == HeapStatus.UNDEFINED);
    long collectionTime = heapStatus.getCollectionTime();
    Assert.assertTrue(collectionTime >= 0 || collectionTime == HeapStatus.UNDEFINED);
    long committed = heapStatus.getCommitted();
    Assert.assertTrue(committed >= 0 || committed == HeapStatus.UNDEFINED);
    long init = heapStatus.getInit();
    Assert.assertTrue(init >= 0 || init == HeapStatus.UNDEFINED);
    long max = heapStatus.getMax();
    // TODO max can end up being -1, is this intended?
    Assert.assertTrue(max >= 0 || max == HeapStatus.UNDEFINED || max == -1);
    long used = heapStatus.getUsed();
    Assert.assertTrue(used >= 0 || used == HeapStatus.UNDEFINED);
  }
}
