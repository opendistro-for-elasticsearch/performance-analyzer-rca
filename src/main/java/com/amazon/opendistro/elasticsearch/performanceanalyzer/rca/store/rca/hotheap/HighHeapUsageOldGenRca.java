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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.OLD_GEN;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_FULL_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.JvmEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This RCA is used to decide whether the old generation in JVM garbage collector is healthy or not.
 * The algorithm is sliding window based and currently is only targeting at CMS garbage collector
 * This RCA subscribes three metrics :the current old gen usage(Heap_Used), the maximum heap size
 * allowed(Heap_Max) and the full GC count during the last time interval(Gc_Event) and the RCA keeps
 * pushing the heap usage and GC count into the sliding window and keeps track of the max heap. When
 * a full GC occurs, this RCA tries to capture the minimum old gen usage right after this full GC
 * and those are the long-lived objects we are interested.The sliding window can check both the sum
 * of gc event and the min heap usage in O(1) time complexity. To check whether the old gen is
 * healthy, we first check the sum of gc event. if it is a non zero value, it means there is at
 * least one full GC during the entire sliding window and then compare the usage with the threshold.
 * To git rid of false positive from sampling, we keep the sliding window big enough to keep at
 * least a couple of such minimum samples to make the min value more accurate.
 */
public class HighHeapUsageOldGenRca extends Rca<ResourceFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(HighHeapUsageOldGenRca.class);
  private int counter;
  private double maxOldGenHeapSize;
  private final Metric heap_Used;
  private final Metric heap_Max;
  private final Metric gc_event;
  private final ResourceType resourceType;
  // the amount of RCA period this RCA needs to run before sending out a flowunit
  private final int rcaPeriod;
  // The lower bound threshold in percentage to decide whether to send out summary.
  // e.g. if lowerBoundThreshold = 0.2, then we only send out summary if value > 0.2*threshold
  private final double lowerBoundThreshold;
  private final SlidingWindow<SlidingWindowData> gcEventSlidingWindow;
  private final MinOldGenSlidingWindow minOldGenSlidingWindow;
  //Keep the sliding window large enough to avoid false positive
  private static final int SLIDING_WINDOW_SIZE_IN_MINS = 10;
  private static final double OLD_GEN_USED_THRESHOLD_IN_PERCENTAGE = 0.65;
  // FullGC needs to occur at least once during the entire sliding window in order to capture the
  // minimum
  private static final double OLD_GEN_GC_THRESHOLD = 1;
  private static final double CONVERT_BYTES_TO_MEGABYTES = Math.pow(1024, 3);
  protected Clock clock;


  public <M extends Metric> HighHeapUsageOldGenRca(final int rcaPeriod, final double lowerBoundThreshold,
      final M heap_Used, final M gc_event, final M heap_Max) {
    super(5);
    this.clock = Clock.systemUTC();
    this.heap_Used = heap_Used;
    this.gc_event = gc_event;
    this.heap_Max = heap_Max;
    this.maxOldGenHeapSize = Double.MAX_VALUE;
    this.rcaPeriod = rcaPeriod;
    this.lowerBoundThreshold = (lowerBoundThreshold >= 0 && lowerBoundThreshold <= 1.0)
        ? lowerBoundThreshold : 1.0;
    this.counter = 0;
    this.resourceType = ResourceType.newBuilder().setJVM(JvmEnum.OLD_GEN).build();
    gcEventSlidingWindow = new SlidingWindow<>(SLIDING_WINDOW_SIZE_IN_MINS, TimeUnit.MINUTES);
    minOldGenSlidingWindow = new MinOldGenSlidingWindow(SLIDING_WINDOW_SIZE_IN_MINS,
        TimeUnit.MINUTES);
  }

  public <M extends Metric> HighHeapUsageOldGenRca(final int rcaPeriod,
      final M heap_Used, final M gc_event, final M heap_Max) {
    this(rcaPeriod, 1.0, heap_Used, gc_event, heap_Max);
  }

  @Override
  public ResourceFlowUnit operate() {
    List<MetricFlowUnit> heapUsedMetrics = heap_Used.getFlowUnits();
    List<MetricFlowUnit> gcEventMetrics = gc_event.getFlowUnits();
    List<MetricFlowUnit> heapMaxMetrics = heap_Max.getFlowUnits();
    double oldGenHeapUsed = Double.NaN;
    int oldGenGCEvent = 0;
    counter += 1;
    for (MetricFlowUnit heapUsedMetric : heapUsedMetrics) {
      if (heapUsedMetric.isEmpty()) {
        continue;
      }
      double ret =
          SQLParsingUtil.readDataFromSqlResult(heapUsedMetric.getData(), MEM_TYPE.getField(), OLD_GEN.toString(), MetricsDB.MAX);
      if (Double.isNaN(ret)) {
        LOG.error("Failed to parse metric in FlowUnit from {}", heap_Used.getClass().getName());
      } else {
        oldGenHeapUsed = ret / CONVERT_BYTES_TO_MEGABYTES;
      }
    }

    for (MetricFlowUnit gcEventMetric : gcEventMetrics) {
      if (gcEventMetric.isEmpty()) {
        continue;
      }
      double ret =
          SQLParsingUtil.readDataFromSqlResult(gcEventMetric.getData(), MEM_TYPE.getField(), TOT_FULL_GC.toString(), MetricsDB.MAX);
      if (Double.isNaN(ret)) {
        LOG.error("Failed to parse metric in FlowUnit from {}", gc_event.getClass().getName());
      } else {
        oldGenGCEvent = (int) ret;
      }
    }

    for (MetricFlowUnit heapMaxMetric : heapMaxMetrics) {
      if (heapMaxMetric.isEmpty()) {
        continue;
      }
      double ret =
          SQLParsingUtil.readDataFromSqlResult(heapMaxMetric.getData(), MEM_TYPE.getField(), OLD_GEN.toString(), MetricsDB.MAX);
      if (Double.isNaN(ret)) {
        LOG.error("Failed to parse metric in FlowUnit from {}", heap_Max.getClass().getName());
      } else {
        maxOldGenHeapSize = ret / CONVERT_BYTES_TO_MEGABYTES;
      }
    }

    if (!Double.isNaN(oldGenHeapUsed)) {
      LOG.debug(
          "oldGenHeapUsed = {}, oldGenGCEvent = {}, maxOldGenHeapSize = {}",
          oldGenHeapUsed,
          oldGenGCEvent,
          maxOldGenHeapSize);
      long currTimeStamp = this.clock.millis();
      gcEventSlidingWindow.next(new SlidingWindowData(currTimeStamp, oldGenGCEvent));
      minOldGenSlidingWindow.next(new SlidingWindowData(currTimeStamp, oldGenHeapUsed));
    }

    if (counter == this.rcaPeriod) {
      ResourceContext context = null;
      HotResourceSummary summary = null;
      // reset the variables
      counter = 0;

      double currentMinOldGenUsage = minOldGenSlidingWindow.readMin();

      if (gcEventSlidingWindow.readSum() >= OLD_GEN_GC_THRESHOLD
          && !Double.isNaN(currentMinOldGenUsage)
          && currentMinOldGenUsage / maxOldGenHeapSize > OLD_GEN_USED_THRESHOLD_IN_PERCENTAGE) {
        LOG.debug("heapUsage is above threshold. OldGGenGCEvent = {}, oldGenUsage percentage = {}",
            gcEventSlidingWindow.readSum(),
            currentMinOldGenUsage / maxOldGenHeapSize);
        context = new ResourceContext(Resources.State.UNHEALTHY);
      } else {
        context = new ResourceContext(Resources.State.HEALTHY);
      }

      //check to see if the value is above lower bound thres
      if (gcEventSlidingWindow.readSum() >= OLD_GEN_GC_THRESHOLD
          && !Double.isNaN(currentMinOldGenUsage)
          && currentMinOldGenUsage / maxOldGenHeapSize > OLD_GEN_USED_THRESHOLD_IN_PERCENTAGE * this.lowerBoundThreshold) {
        summary = new HotResourceSummary(this.resourceType,
            OLD_GEN_USED_THRESHOLD_IN_PERCENTAGE, currentMinOldGenUsage / maxOldGenHeapSize, SLIDING_WINDOW_SIZE_IN_MINS * 60);
      }

      LOG.debug("High Heap Usage RCA Context = " + context.toString());
      return new ResourceFlowUnit(this.clock.millis(), context, summary);
    } else {
      // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA
      // state)
      LOG.debug("Empty FlowUnit returned for High Heap Usage RCA");
      return new ResourceFlowUnit(this.clock.millis());
    }
  }

  /**
   * Sliding window to check the minimal olg gen usage within a given time frame
   */
  private static class MinOldGenSlidingWindow extends SlidingWindow<SlidingWindowData> {

    public MinOldGenSlidingWindow(int SLIDING_WINDOW_SIZE_IN_TIMESTAMP, TimeUnit timeUnit) {
      super(SLIDING_WINDOW_SIZE_IN_TIMESTAMP, timeUnit);
    }

    @Override
    public void next(SlidingWindowData e) {
      while (!windowDeque.isEmpty()
          && windowDeque.peekFirst().getValue() >= e.getValue()) {
        windowDeque.pollFirst();
      }
      windowDeque.addFirst(e);
      while (!windowDeque.isEmpty()
          &&
          TimeUnit.MILLISECONDS.toSeconds(e.getTimeStamp() - windowDeque.peekLast().getTimeStamp())
              > SLIDING_WINDOW_SIZE) {
        windowDeque.pollLast();
      }
    }

    public double readMin() {
      if (!windowDeque.isEmpty()) {
        return windowDeque.peekLast().getValue();
      }
      return Double.NaN;
    }
  }

  /**
   * This is a local node RCA which by definition can not be serialize/de-serialized
   * over gRPC.
   */
  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    LOG.error("RCA: {} should not be send over from network", this.getClass().getSimpleName());
  }
}
