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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import org.junit.Assert;
import org.junit.Before;

public class MountedPartitionMetricsCollectorTest extends AbstractCollectorTest {
  @Before
  public void setup() {
    setUut(new MountedPartitionMetricsCollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception {
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
