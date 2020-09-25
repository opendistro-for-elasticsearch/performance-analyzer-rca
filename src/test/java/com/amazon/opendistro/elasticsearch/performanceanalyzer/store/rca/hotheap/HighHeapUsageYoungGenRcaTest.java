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
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_YOUNG_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;
import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageYoungGenRca;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.junit.experimental.categories.Category;


@Category(GradleTaskForRca.class)
@Ignore
public class HighHeapUsageYoungGenRcaTest {
  private static final double CONVERT_MEGABYTES_TO_BYTES = Math.pow(1024, 2);
  private MetricTestHelper heap_Used;
  private MetricTestHelper gc_Collection_Time;
  private HighHeapUsageYoungGenRcaX youngGenRcaX;
  private List<String> columnName;

  /**
   * generate flowunit and bind the flowunits it generate to metrics
   */
  private void mockFlowUnits(double heapUsageVal, double gcCollectionTimeVal) {
    heap_Used.createTestFlowUnits(columnName, Arrays.asList(OLD_GEN.toString(), String.valueOf(heapUsageVal * CONVERT_MEGABYTES_TO_BYTES)));
    gc_Collection_Time.createTestFlowUnits(columnName, Arrays.asList(TOT_YOUNG_GC.toString(), String.valueOf(gcCollectionTimeVal)));
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

    //ts = 0, heap = 0, gc time = 0
    mockFlowUnits(0, 0);
    youngGenRcaX.setClock(constantClock);
    flowUnit = youngGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 1, heap = 450MB, gc time = 200ms
    mockFlowUnits(450, 200);
    youngGenRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(1)));
    flowUnit = youngGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 2, heap = 1050MB, gc time = 400ms
    mockFlowUnits(1050, 400);
    youngGenRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(2)));
    flowUnit = youngGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 3, heap = 1550MB, gc time = 650ms
    mockFlowUnits(1550, 650);
    youngGenRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(3)));
    flowUnit = youngGenRcaX.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
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
