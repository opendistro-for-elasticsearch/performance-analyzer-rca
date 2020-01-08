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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.OLD_GEN;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_FULL_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit.HighHeapUsageFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
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
public class HighHeapUsageOldGenRca extends Rca<HighHeapUsageFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(HighHeapUsageOldGenRca.class);
  private static final int RCA_PERIOD = 12;
  private int counter;
  private double maxOldGenHeapSize;
  private final Metric heap_Used;
  private final Metric heap_Max;
  private final Metric gc_event;
  private final SamplingDataSlidingWindow samplingDataSlidingWindow;
  // Keep the sliding window large enough to avoid false positive
  private static final int SLIDING_WINDOW_SIZE_IN_MINS = 10;
  private static final double OLD_GEN_USED_THRESHOLD_IN_PERCENTAGE = 0.65;
  // FullGC needs to occur at least once during the entire sliding window in order to capture the
  // minimum
  private static final double OLD_GEN_GC_THRESHOLD = 1;
  private static final double CONVERT_BYTES_TO_MEGABYTES = Math.pow(1024, 3);

  public <M extends Metric> HighHeapUsageOldGenRca(
      long evaluationIntervalSeconds, final M heap_Used, final M gc_event, final M heap_Max) {
    super(evaluationIntervalSeconds);
    this.heap_Used = heap_Used;
    this.gc_event = gc_event;
    this.heap_Max = heap_Max;
    maxOldGenHeapSize = Double.MAX_VALUE;
    counter = 0;
    samplingDataSlidingWindow = new SamplingDataSlidingWindow(SLIDING_WINDOW_SIZE_IN_MINS);
  }

  public ResourceContext determineHeapUsageState() {
    double currentMinOldGenUsage = samplingDataSlidingWindow.getMinOldGenUsage();
    if (samplingDataSlidingWindow.getOldGGenGCEvent() >= OLD_GEN_GC_THRESHOLD
        && !Double.isNaN(currentMinOldGenUsage)
        && currentMinOldGenUsage / maxOldGenHeapSize > OLD_GEN_USED_THRESHOLD_IN_PERCENTAGE) {
      LOG.debug(
          "heapUsage is above threshold. OldGGenGCEvent = {}, oldGenUsage percentage = {}",
          samplingDataSlidingWindow.getOldGGenGCEvent(),
          currentMinOldGenUsage / maxOldGenHeapSize);
      return new ResourceContext(ResourceContext.Resource.HEAP, ResourceContext.State.UNHEALTHY);
    }
    return new ResourceContext(ResourceContext.Resource.HEAP, ResourceContext.State.HEALTHY);
  }

  @Override
  public HighHeapUsageFlowUnit operate() {
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
          heapUsedMetric.getDataFromMetric(MEM_TYPE.toString(), OLD_GEN.toString(), MetricsDB.MAX);
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
          gcEventMetric.getDataFromMetric(
              MEM_TYPE.toString(), TOT_FULL_GC.toString(), MetricsDB.MAX);
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
          heapMaxMetric.getDataFromMetric(MEM_TYPE.toString(), OLD_GEN.toString(), MetricsDB.MAX);
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
      samplingDataSlidingWindow.add(System.currentTimeMillis(), oldGenHeapUsed, oldGenGCEvent);
    }

    if (counter == RCA_PERIOD) {
      List<List<String>> ret = new ArrayList<>();
      ClusterDetailsEventProcessor.NodeDetails currentNode = ClusterDetailsEventProcessor
          .getCurrentNodeDetails();
      if (currentNode != null) {
        ret.addAll(Arrays.asList(Collections.singletonList("Node ID"),
            Collections.singletonList(currentNode.getId())));
      } else {
        ret.addAll(Arrays
            .asList(Collections.singletonList("Node ID"), Collections.singletonList("unknown")));
      }
      ResourceContext context = determineHeapUsageState();
      // reset the variables
      counter = 0;
      LOG.info("High Heap Usage RCA Context = " + context.toString());
      return new HighHeapUsageFlowUnit(System.currentTimeMillis(), ret, context);
    } else {
      // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA
      // state)
      LOG.debug("Empty FlowUnit returned for High Heap Usage RCA");
      return new HighHeapUsageFlowUnit(System.currentTimeMillis(), ResourceContext.generic());
    }
  }

  private static class SamplingDataSlidingWindow {

    Deque<Pair<Long, Double>> oldGenUsageDeque;
    Deque<Pair<Long, Integer>> oldGGenGCEventDeque;
    private final int SLIDING_WINDOW_SIZE_IN_MINS;

    SamplingDataSlidingWindow(int SLIDING_WINDOW_SIZE_IN_MINS) {
      this.SLIDING_WINDOW_SIZE_IN_MINS = SLIDING_WINDOW_SIZE_IN_MINS;
      this.oldGenUsageDeque = new LinkedList<>();
      this.oldGGenGCEventDeque = new LinkedList<>();
    }

    public void add(long timeStamp, double oldGenUsage, int oldGenGCEvent) {
      if (oldGenGCEvent > 0) {
        oldGGenGCEventDeque.addFirst(Pair.of(timeStamp, oldGenGCEvent));
      }
      while (!oldGGenGCEventDeque.isEmpty()
          && TimeUnit.MILLISECONDS.toSeconds(timeStamp - oldGGenGCEventDeque.peekLast().getKey())
          > SLIDING_WINDOW_SIZE_IN_MINS * 60) {
        oldGGenGCEventDeque.pollLast();
      }

      while (!oldGenUsageDeque.isEmpty()
          && oldGenUsageDeque.peekFirst().getValue() >= oldGenUsage) {
        oldGenUsageDeque.pollFirst();
      }
      oldGenUsageDeque.addFirst(Pair.of(timeStamp, oldGenUsage));
      while (!oldGenUsageDeque.isEmpty()
          && TimeUnit.MILLISECONDS.toSeconds(timeStamp - oldGenUsageDeque.peekLast().getKey())
          > SLIDING_WINDOW_SIZE_IN_MINS * 60) {
        oldGenUsageDeque.pollLast();
      }
    }

    public int getOldGGenGCEvent() {
      return oldGGenGCEventDeque.size();
    }

    public double getMinOldGenUsage() {
      if (!oldGenUsageDeque.isEmpty()) {
        return oldGenUsageDeque.peekLast().getValue();
      }
      return Double.NaN;
    }
  }

  /**
   * TODO: Move this method out of the RCA class. The scheduler should set the flow units it drains
   * from the Rx queue between the scheduler and the networking thread into the node.
   *
   * @param args The wrapper around the flow unit operation.
   */
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitWrapper> flowUnitWrappers =
        args.getWireHopper().readFromWire(args.getNode());
    List<HighHeapUsageFlowUnit> flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitWrapper messageWrapper : flowUnitWrappers) {
      flowUnitList.add(HighHeapUsageFlowUnit.buildFlowUnitFromWrapper(messageWrapper));
    }

    setFlowUnits(flowUnitList);
  }
}
