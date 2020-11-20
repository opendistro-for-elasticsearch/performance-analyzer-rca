package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import org.junit.Assert;
import org.junit.Before;

public class MountedPartitionMetricsCollectorTest extends AbstractCollectorTest {
  @Before
  public void setup() {
    setUut(new MountedPartitionMetricsCollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception{
    MountedPartitionMetrics partitionMetrics = mapper.readValue(metric, MountedPartitionMetrics.class);
    // TODO implement further validation of the MetricStatus
    Assert.assertFalse(partitionMetrics.getMountPoint().isEmpty());
    Assert.assertFalse(partitionMetrics.getDevicePartition().isEmpty());
    long totalSpace = partitionMetrics.getTotalSpace();
    Assert.assertTrue(totalSpace >= 0 || totalSpace == -1);
    long freeSpace = partitionMetrics.getFreeSpace();
    Assert.assertTrue(freeSpace >= 0 || freeSpace == -1);
    long usableFreeSpace = partitionMetrics.getUsableFreeSpace();
    Assert.assertTrue(usableFreeSpace >= 0 || usableFreeSpace == -1);
  }
}
