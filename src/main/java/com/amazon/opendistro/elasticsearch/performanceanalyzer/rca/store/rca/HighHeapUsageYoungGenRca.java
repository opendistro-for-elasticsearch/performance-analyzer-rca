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
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_YOUNG_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit.HighHeapUsageFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This RCA is used to decide whether the young generation in CMS garbage collector is healthy or
 * not. It subscribes two different metrics : heap_usage and gc_collection_time. For
 * gc_collection_time we simply measure its moving average. And we use heap_used to measure the
 * increase in old gen usage for each sampling to decide the amount of objects being promoted to old
 * gen during the last time interval and then use it to calculate its moving average. If both the
 * promotion rate and young gen GC time reach the threshold, this node is marked as unhealthy.
 */
public class HighHeapUsageYoungGenRca extends Rca<ResourceFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(HighHeapUsageYoungGenRca.class);
  private static final int RCA_PERIOD = 12;
  private static final int PROMOTION_RATE_SLIDING_WINDOW_IN_MINS = 10;
  //promotion rate threshold is 500 Mb/s
  private static final double PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC = 500;
  //young gc time threshold is 400 ms per second
  private static final double YOUNG_GC_TIME_THRESHOLD_IN_MS_PER_SEC = 400;
  private static final double CONVERT_BYTES_TO_MEGABYTES = Math.pow(1024, 2);

  protected int counter;
  private final Metric heap_Used;
  private final Metric gc_Collection_Time;
  private final SlidingWindow gcTimeDeque;
  private final SlidingWindow promotionRateDeque;

  public <M extends Metric> HighHeapUsageYoungGenRca(long evaluationIntervalSeconds, final M heap_Used,
      final M gc_Collection_Time) {
    super(evaluationIntervalSeconds);
    counter = 0;
    this.heap_Used = heap_Used;
    this.gc_Collection_Time = gc_Collection_Time;
    this.gcTimeDeque = new SlidingWindow(PROMOTION_RATE_SLIDING_WINDOW_IN_MINS * 60);

    this.promotionRateDeque = new SlidingWindow(PROMOTION_RATE_SLIDING_WINDOW_IN_MINS * 60) {
      /**
       * always compare the current old gen usage with the usage from the previous time intervals and the amount of
       * increase is the data that is promoted from young gen.
       <p>
       * For example :
       *                                     Full GC
       *                                       |
       * old gen(in Mb): 18500 | 18600 | 18700 | 3000 | 3050 | 3100
       * data promoted:   n/a  |  100  |  100  |  0   |  50  |  50
       </p>
       * so the promotion rate within this time window is (100 + 100 + 0 + 50 + 50) / time slice
       */
      @Override
      protected void add(double value) {
        if (!windowDeque.isEmpty() && value > windowDeque.peekFirst().getValue()) {
          sum += (value - windowDeque.peekFirst().getValue());
        }
      }

      @Override
      protected void remove(double value) {
        if (!windowDeque.isEmpty() && value < windowDeque.peekFirst().getValue()) {
          sum -= (windowDeque.peekFirst().getValue() - value);
        }
      }
    };
  }

  private ResourceContext determineHeapUsageState() {
    double avgPromotionRate = promotionRateDeque.read();
    double avgYoungGCTime = gcTimeDeque.read();
    LOG.debug("avgPromotionRate = {} , avgGCTime = {}", avgPromotionRate, avgYoungGCTime);
    if (!Double.isNaN(avgPromotionRate) && avgPromotionRate > PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC
        && !Double.isNaN(avgYoungGCTime)
        && avgYoungGCTime > YOUNG_GC_TIME_THRESHOLD_IN_MS_PER_SEC) {
      return new ResourceContext(ResourceContext.Resource.HEAP, ResourceContext.State.UNHEALTHY);
    }
    return new ResourceContext(ResourceContext.Resource.HEAP, ResourceContext.State.HEALTHY);
  }

  @Override
  public ResourceFlowUnit operate() {
    List<MetricFlowUnit> heapUsedMetrics = heap_Used.getFlowUnits();
    List<MetricFlowUnit> gcCollectionTimeMetrics = gc_Collection_Time.getFlowUnits();

    double totYoungGCTime = Double.NaN;
    double oldGenHeapUsed = Double.NaN;
    counter += 1;

    if (!heapUsedMetrics.isEmpty() && !heapUsedMetrics.get(0).isEmpty()) {
      double ret = heapUsedMetrics.get(0)
          .getDataFromMetric(MEM_TYPE.toString(), OLD_GEN.toString(), MetricsDB.MAX);
      if (Double.isNaN(ret)) {
        LOG.error("Failed to parse metric in FlowUnit from {}", heap_Used.getClass().getName());
      } else {
        oldGenHeapUsed = ret / CONVERT_BYTES_TO_MEGABYTES;
      }
    }

    if (!gcCollectionTimeMetrics.isEmpty() && !gcCollectionTimeMetrics.get(0).isEmpty()) {
      double ret = gcCollectionTimeMetrics.get(0)
          .getDataFromMetric(MEM_TYPE.toString(), TOT_YOUNG_GC.toString(), MetricsDB.MAX);
      if (Double.isNaN(ret)) {
        LOG.error("Failed to parse metric in FlowUnit from {}",
            gc_Collection_Time.getClass().getName());
      } else {
        totYoungGCTime = ret;
      }
    }

    if (!Double.isNaN(oldGenHeapUsed)) {
      promotionRateDeque.next(System.currentTimeMillis(), oldGenHeapUsed);
    }
    if (!Double.isNaN(totYoungGCTime)) {
      gcTimeDeque.next(System.currentTimeMillis(), totYoungGCTime);
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
      LOG.debug("Young Gen RCA Context = " + context.toString());
      return new ResourceFlowUnit(System.currentTimeMillis(), ret, context);
    } else {
      // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA state)
      LOG.debug("RCA: Empty FlowUnit returned for Young Gen RCA");
      return new ResourceFlowUnit(System.currentTimeMillis(), ResourceContext.generic());
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
    List<ResourceFlowUnit> flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitWrapper messageWrapper : flowUnitWrappers) {
      flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(messageWrapper));
    }

    setFlowUnits(flowUnitList);
  }
}
