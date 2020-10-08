package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator;

import java.util.Set;

public interface MountedPartitionMetricsGenerator {
  void addSample();

  Set<String> getAllMountPoints();

  String getDevicePartition(String mountPoint);

  long getTotalSpace(String mountPoint);

  long getFreeSpace(String mountPoint);

  long getUsableFreeSpace(String mountPoint);
}
