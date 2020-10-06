package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.linux;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MountedPartitionMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet.MountedPartitions;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.MountedPartitionMetricsGenerator;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LinuxMountedPartitionMetricsGenerator implements MountedPartitionMetricsGenerator {
  private static final Map<String, MountedPartitionMetrics> suppliers = new HashMap<>();

  @Override
  public void addSample() {
    MountedPartitions.addSample();
  }

  @Override
  public Set<String> getAllMountPoints() {
    return ImmutableSet.copyOf(suppliers.keySet());
  }

  public void addSupplier(final String mountPoint,
      final MountedPartitionMetrics supplier) {
    suppliers.put(mountPoint, supplier);
  }

  @Override
  public String getDevicePartition(final String mountPoint) {
    return suppliers.get(mountPoint).getDevicePartition();
  }

  @Override
  public long getTotalSpace(final String mountPoint) {
    return suppliers.get(mountPoint).getTotalSpace();
  }

  @Override
  public long getFreeSpace(final String mountPoint) {
    return suppliers.get(mountPoint).getFreeSpace();
  }

  @Override
  public long getUsableFreeSpace(final String mountPoint) {
    return suppliers.get(mountPoint).getUsableFreeSpace();
  }
}
