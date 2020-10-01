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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.hotheap;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.OLD_GEN;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_FULL_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_YOUNG_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.FULL_GC_PAUSE_TIME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.MINOR_GC_PAUSE_TIME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.YOUNG_GEN_PROMOTION_RATE;
import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageYoungGenRca;

import com.google.common.collect.Lists;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.junit.experimental.categories.Category;


@Category(GradleTaskForRca.class)
public class HighHeapUsageYoungGenRcaTest {
  private static final double CONVERT_MEGABYTES_TO_BYTES = Math.pow(1024, 2);
  private MetricTestHelper heap_Used;
  private MetricTestHelper gc_Collection_Time;
  private HighHeapUsageYoungGenRcaX youngGenRcaX;
  private List<String> columnName;

  /**
   * generates mock metric flow units that RCAs can consume
   */
  private void mockFlowUnits(double heapUsageVal, double minorGcTime, double fullGcTime) {
    heap_Used.createTestFlowUnits(columnName, Arrays.asList(OLD_GEN.toString(), String.valueOf(heapUsageVal * CONVERT_MEGABYTES_TO_BYTES)));
    List<String> youngGcRow = Arrays.asList(TOT_YOUNG_GC.toString(), String.valueOf(minorGcTime));
    List<String> fullGcRow = Arrays.asList(TOT_FULL_GC.toString(), String.valueOf(fullGcTime));
    gc_Collection_Time.createTestFlowUnitsWithMultipleRows(columnName, Lists.newArrayList(youngGcRow, fullGcRow));
  }

  @Before
  public void initTestHighHeapYoungGenRca() {
    heap_Used = new MetricTestHelper(5);
    gc_Collection_Time = new MetricTestHelper(5);
    youngGenRcaX = new HighHeapUsageYoungGenRcaX(1, heap_Used, gc_Collection_Time);
    columnName = Arrays.asList(MEM_TYPE.toString(), MetricsDB.MAX);
  }

  @Test
  public void testHighHeapYoungGenRca() {
    ResourceFlowUnit flowUnit;
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

    // ts = 0, heap = 0, minor gc time = 0, full gc time = 0ms
    mockFlowUnits(0, 0, 0);
    youngGenRcaX.setClock(constantClock);
    flowUnit = youngGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    // ts = 1, heap = 450MB, minor gc time = 200ms, full gc time = 9000ms
    mockFlowUnits(450, 200, 9000);
    youngGenRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(1)));
    flowUnit = youngGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    // ts = 2, heap = 1050MB, minor gc time = 240ms, full gc time = 12000ms
    // the average full GC time is now 10.5s which is > the threshold of 10s
    mockFlowUnits(1050, 400, 12000);
    youngGenRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(2)));
    flowUnit = youngGenRcaX.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    HotResourceSummary summary = (HotResourceSummary) flowUnit.getSummary();
    Assert.assertEquals(FULL_GC_PAUSE_TIME, summary.getResource());
    Assert.assertEquals(10500, summary.getValue(), 0.1);

    // ts = 3, heap = 1650MB, minor gc time = 600ms, full gc time = 0ms
    // the average promotion rate is now 550 MB/s which is > the threshold of 500 MB/s
    mockFlowUnits(1650, 600, 0);
    youngGenRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(3)));
    flowUnit = youngGenRcaX.operate();
    summary = (HotResourceSummary) flowUnit.getSummary();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(YOUNG_GEN_PROMOTION_RATE, summary.getResource());
    Assert.assertEquals(550, summary.getValue(), 0.1);

    // ts = 4, heap = 1650MB, minor gc time = 800ms, full gc time = 0ms
    // the average minor gc time is now 500 ms / sec which is > the threshold of 400 ms / sec
    mockFlowUnits(1650, 800, 0);
    youngGenRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(4)));
    flowUnit = youngGenRcaX.operate();
    summary = (HotResourceSummary) flowUnit.getSummary();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(MINOR_GC_PAUSE_TIME, summary.getResource());
    Assert.assertEquals(500, summary.getValue(), 0.1);

    // ts = 5, heap = 0MB, minor gc time = 0ms, full gc time = 0ms
    // the average garbage promotion percent is now 100% which is > the threshold of 80%
    mockFlowUnits(0, 0, 0);
    youngGenRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(5)));
    flowUnit = youngGenRcaX.operate();
    summary = (HotResourceSummary) flowUnit.getSummary();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(YOUNG_GEN_PROMOTION_RATE, summary.getResource());
    Assert.assertEquals(1, summary.getValue(), 0.1);
  }

  private static class HighHeapUsageYoungGenRcaX extends HighHeapUsageYoungGenRca {
    public <M extends Metric> HighHeapUsageYoungGenRcaX(final int rcaPeriod,
        final M heap_Used, final M gc_Collection_Time) {
      super(rcaPeriod, heap_Used, gc_Collection_Time);
    }

    public void setClock(Clock testClock) {
      this.clock = testClock;
    }
  }
}
