/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class OldGenRca<T extends ResourceFlowUnit<?>> extends Rca<T> {

  private static final Logger LOG = LogManager.getLogger(OldGenRca.class);
  private static final double CONVERT_BYTES_TO_MEGABYTES = Math.pow(1024, 2);
  protected final Metric heap_Used;
  protected final Metric heap_Max;
  protected final Metric gc_event;

  public OldGenRca(long evaluationIntervalSeconds, Metric heapUsed, Metric heapMax,
      Metric gcEvent) {
    super(evaluationIntervalSeconds);
    this.heap_Max = heapMax;
    this.heap_Used = heapUsed;
    this.gc_event = gcEvent;
  }

  protected double getMaxOldGenSizeOrDefault(final double defaultValue) {
    if (heap_Max == null) {
      throw new IllegalStateException("RCA: " + this.name() + "was not configured in the graph to "
          + "take heap_Max as a metric. Please check the analysis graph!");
    }

    double maxOldGenHeapSize = defaultValue;
    final List<MetricFlowUnit> heapMaxMetrics = heap_Max.getFlowUnits();
    for (MetricFlowUnit heapMaxMetric : heapMaxMetrics) {
      if (heapMaxMetric.isEmpty()) {
        continue;
      }
      double ret =
          SQLParsingUtil
              .readDataFromSqlResult(heapMaxMetric.getData(), MEM_TYPE.getField(), OLD_GEN.toString(), MetricsDB.MAX);
      if (Double.isNaN(ret)) {
        LOG.error("Failed to parse metric in FlowUnit from {}", heap_Max.getClass().getName());
      } else {
        maxOldGenHeapSize = ret / CONVERT_BYTES_TO_MEGABYTES;
      }
    }

    return maxOldGenHeapSize;
  }

  protected int getFullGcEventsOrDefault(final double defaultValue) {
    if (gc_event == null) {
      throw new IllegalStateException("RCA: " + this.name() + "was not configured in the graph to "
          + "take gc_event as a metric. Please check the analysis graph!");
    }

    double fullGcEvents = defaultValue;
    final List<MetricFlowUnit> gcEventMetrics = gc_event.getFlowUnits();

    for (MetricFlowUnit gcEventMetric : gcEventMetrics) {
      if (gcEventMetric.isEmpty()) {
        continue;
      }
      double ret =
          SQLParsingUtil.readDataFromSqlResult(gcEventMetric.getData(), MEM_TYPE.getField(), TOT_FULL_GC.toString(), MetricsDB.MAX);
      if (Double.isNaN(ret)) {
        LOG.error("Failed to parse metric in FlowUnit from {}", gc_event.getClass().getName());
      } else {
        fullGcEvents = ret;
      }
    }

    return (int) fullGcEvents;
  }

  protected double getOldGenUsedOrDefault(final double defaultValue) {
    if (heap_Used == null) {
      throw new IllegalStateException("RCA: " + this.name() + "was not configured in the graph to "
          + "take heap_Used as a metric. Please check the analysis graph!");
    }

    final List<MetricFlowUnit> heapUsedMetrics = heap_Used.getFlowUnits();
    double oldGenHeapUsed = defaultValue;
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

    return oldGenHeapUsed;
  }

  /**
   * Sliding window to check the minimal olg gen usage within a given time frame
   */
  public static class MinOldGenSlidingWindow extends SlidingWindow<SlidingWindowData> {

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
}
