package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DevicePartitionDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DevicePartitionValue;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MountedPartitionMetrics extends MetricStatus {
  private String mountPoint;
  private String devicePartition;
  private long totalSpace;
  private long freeSpace;
  private long usableFreeSpace;

  public MountedPartitionMetrics() {}

  public MountedPartitionMetrics(String devicePartition, String mountPoint, long totalSpace,
      long freeSpace, long usableFreeSpace) {
    this.devicePartition = devicePartition;
    this.mountPoint = mountPoint;
    this.totalSpace = totalSpace;
    this.freeSpace = freeSpace;
    this.usableFreeSpace = usableFreeSpace;
  }

  @JsonProperty(DevicePartitionDimension.Constants.MOUNT_POINT_VALUE)
  public String getMountPoint() {
    return mountPoint;
  }

  @JsonProperty(DevicePartitionDimension.Constants.DEVICE_PARTITION_VALUE)
  public String getDevicePartition() {
    return devicePartition;
  }

  @JsonProperty(DevicePartitionValue.Constants.TOTAL_SPACE_VALUE)
  public long getTotalSpace() {
    return totalSpace;
  }

  @JsonProperty(DevicePartitionValue.Constants.FREE_SPACE_VALUE)
  public long getFreeSpace() {
    return freeSpace;
  }

  @JsonProperty(DevicePartitionValue.Constants.USABLE_FREE_SPACE_VALUE)
  public long getUsableFreeSpace() {
    return usableFreeSpace;
  }
}
