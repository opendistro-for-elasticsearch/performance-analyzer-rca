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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.OLD_GEN;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_FULL_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;
import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HighHeapUsageOldGenRcaTest {
  private static final double CONVERT_MEGABYTES_TO_BYTES = Math.pow(1024, 2);
  private MetricTestHelper heap_Used;
  private MetricTestHelper gc_event;
  private MetricTestHelper heap_Max;
  private List<Metric> node_stats;
  private HighHeapUsageOldGenRcaX oldGenRcaX;
  private List<String> columnName;

  /**
   * generate flowunit and bind the flowunits it generate to metrics
   */
  private void mockFlowUnits(double heapUsageVal, int gcEventVal) {
    heap_Used.createTestFlowUnits(columnName, Arrays.asList(OLD_GEN.toString(), String.valueOf(heapUsageVal * CONVERT_MEGABYTES_TO_BYTES)));
    gc_event.createTestFlowUnits(columnName, Arrays.asList(TOT_FULL_GC.toString(), String.valueOf(gcEventVal)));
  }

  @Before
  public void initTestHighHeapOldGenRca() {
    heap_Used = new MetricTestHelper(5);
    gc_event = new MetricTestHelper(5);
    heap_Max = new MetricTestHelper(5);
    node_stats = new ArrayList<Metric>() {{
      add(new MetricTestHelper(5));
    }};
    oldGenRcaX = new HighHeapUsageOldGenRcaX(1, heap_Used, gc_event, heap_Max, node_stats);
    columnName = Arrays.asList(MEM_TYPE.toString(), MetricsDB.MAX);
    // set max heap size to 100MB
    heap_Max.createTestFlowUnits(columnName, Arrays.asList(OLD_GEN.toString(), String.valueOf(100 * CONVERT_MEGABYTES_TO_BYTES)));
  }

  @Test
  public void testHighHeapOldGenRca() {
    ResourceFlowUnit flowUnit;
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

    //ts = 0, heap = 50Mb, full gc = 0
    mockFlowUnits(50, 0);
    oldGenRcaX.setClock(constantClock);
    flowUnit = oldGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 3m, heap = 95MB, full gc = 0
    mockFlowUnits(95, 0);
    oldGenRcaX.setClock(Clock.offset(constantClock, Duration.ofMinutes(3)));
    flowUnit = oldGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 7m, heap = 35MB, full gc = 1
    mockFlowUnits(35, 1);
    oldGenRcaX.setClock(Clock.offset(constantClock, Duration.ofMinutes(7)));
    flowUnit = oldGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 12m, heap = 85MB, full gc = 0
    mockFlowUnits(85, 0);
    oldGenRcaX.setClock(Clock.offset(constantClock, Duration.ofMinutes(12)));
    flowUnit = oldGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 15m, heap = 75MB, full gc = 1
    mockFlowUnits(75, 1);
    oldGenRcaX.setClock(Clock.offset(constantClock, Duration.ofMinutes(15)));
    flowUnit = oldGenRcaX.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 20m, heap = 80MB, full gc = 0
    mockFlowUnits(80, 0);
    oldGenRcaX.setClock(Clock.offset(constantClock, Duration.ofMinutes(20)));
    flowUnit = oldGenRcaX.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
  }

  private static class HighHeapUsageOldGenRcaX extends HighHeapUsageOldGenRca {
    public <M extends Metric> HighHeapUsageOldGenRcaX(final int rcaPeriod,
        final M heap_Used, final M gc_event, final M heap_Max, final List<Metric> node_stats) {
      super(rcaPeriod, heap_Used, gc_event, heap_Max, node_stats);
    }

    public void setClock(Clock testClock) {
      this.clock = testClock;
    }
  }
}
