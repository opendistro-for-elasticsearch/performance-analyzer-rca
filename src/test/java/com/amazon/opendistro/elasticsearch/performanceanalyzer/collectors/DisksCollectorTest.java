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

public class DisksCollectorTest extends AbstractCollectorTest {
  @Before
  public void setup() {
    setUut(new DisksCollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception {
    DiskMetrics diskMetrics = mapper.readValue(metric, DiskMetrics.class);
    // TODO implement further validation of the MetricStatus
    Assert.assertFalse(diskMetrics.getName().isEmpty());
    Assert.assertTrue(diskMetrics.getUtilization() >= 0 && diskMetrics.getUtilization() <= 1);
    Assert.assertTrue(diskMetrics.getAwait() >= 0);
    Assert.assertTrue(diskMetrics.getServiceRate() >= 0);
  }
}
