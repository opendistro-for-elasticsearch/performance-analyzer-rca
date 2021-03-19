/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
