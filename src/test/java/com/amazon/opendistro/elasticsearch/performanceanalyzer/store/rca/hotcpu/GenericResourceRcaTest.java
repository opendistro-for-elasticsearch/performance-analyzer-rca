/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.hotcpu;

import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.categories.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hot_node.HighCpuRca;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class GenericResourceRcaTest {


  @Test
  public void testHighTotalCpuRca() {
    List<String> columnName = Arrays.asList(CommonDimension.OPERATION.toString(), MetricsDB.AVG);
    MetricTestHelper cpuUtilizationGroupByOperation = new MetricTestHelper(5);
    // threshold is 0.7, lower bound is 0.7*0.5 = 0.35
    HighCpuRcaX highTotalCpuRcaX = new HighCpuRcaX(1, cpuUtilizationGroupByOperation);
    highTotalCpuRcaX.setThreshold(0.7);
    highTotalCpuRcaX.setLowerBoundThreshold(0.35);

    ResourceFlowUnit flowUnit;
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

    // ts = 0, cpu = [0.2]
    cpuUtilizationGroupByOperation.createTestFlowUnits(columnName, Arrays.asList("App1", "0.2"));
    highTotalCpuRcaX.setClock(constantClock);
    flowUnit = highTotalCpuRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    // ts = 3, cpu = [0.2, 0.6]
    // above lower bound, start to send summary
    cpuUtilizationGroupByOperation.createTestFlowUnits(columnName, Arrays.asList("App1", "0.6"));
    highTotalCpuRcaX.setClock(Clock.offset(constantClock, Duration.ofMinutes(3)));
    flowUnit = highTotalCpuRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertTrue(flowUnit.hasResourceSummary());

    // ts = 11, cpu = [0.6, 0.8]
    // above lower bound, start to send summary
    cpuUtilizationGroupByOperation.createTestFlowUnits(columnName, Arrays.asList("App1", "0.8"));
    highTotalCpuRcaX.setClock(Clock.offset(constantClock, Duration.ofMinutes(11)));
    flowUnit = highTotalCpuRcaX.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertTrue(flowUnit.hasResourceSummary());

    // ts = 15, cpu = [0.8, 0.2]
    // above lower bound, start to send summary
    cpuUtilizationGroupByOperation.createTestFlowUnits(columnName, Arrays.asList("App1", "0.2"));
    highTotalCpuRcaX.setClock(Clock.offset(constantClock, Duration.ofMinutes(11)));
    flowUnit = highTotalCpuRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertTrue(flowUnit.hasResourceSummary());
  }

  private static class HighCpuRcaX extends HighCpuRca {
    public <M extends Metric> HighCpuRcaX(final int rcaPeriod, final M cpuUtilizationGroupByOperation) {
      super(rcaPeriod, cpuUtilizationGroupByOperation);
    }

    public void setClock(Clock testClock) {
      this.clock = testClock;
    }
  }
}
