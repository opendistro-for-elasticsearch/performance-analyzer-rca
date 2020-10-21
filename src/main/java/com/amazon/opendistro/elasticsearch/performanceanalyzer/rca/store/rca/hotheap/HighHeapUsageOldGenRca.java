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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.OLD_GEN_HEAP_USAGE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageOldGenRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.TopConsumerSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaVerticesMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.OldGenRca;
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
 <p>
 * This RCA read the following node stats from metric and sort them to get the list of top consumers
 * cache :
 * Cache_FieldData_Size / Cache_Request_Size / Cache_Query_Size
 * Lucene memory :
 * Segments_Memory / Terms_Memory / StoredFields_Memory / TermVectors_Memory / Norms_Memory
 * Points_Memory / DocValues_Memory / IndexWriter_Memory / Bitset_Memory / VersionMap_Memory
 </p>
 */
public class HighHeapUsageOldGenRca extends OldGenRca<ResourceFlowUnit<HotResourceSummary>> {

  private static final Logger LOG = LogManager.getLogger(HighHeapUsageOldGenRca.class);
  private int counter;
  private double maxOldGenHeapSize;
  //list of node stat aggregator to collect node stats
  private final List<NodeStatAggregator> nodeStatAggregators;
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
  private int topK;
  protected Clock clock;


  public <M extends Metric> HighHeapUsageOldGenRca(final int rcaPeriod, final double lowerBoundThreshold,
      final M heap_Used, final M gc_event, final M heap_Max, final List<Metric> consumers) {
    super(5, heap_Used, heap_Max, gc_event);
    this.clock = Clock.systemUTC();
    this.maxOldGenHeapSize = Double.MAX_VALUE;
    this.rcaPeriod = rcaPeriod;
    this.lowerBoundThreshold = (lowerBoundThreshold >= 0 && lowerBoundThreshold <= 1.0)
        ? lowerBoundThreshold : 1.0;
    this.counter = 0;
    gcEventSlidingWindow = new SlidingWindow<>(SLIDING_WINDOW_SIZE_IN_MINS, TimeUnit.MINUTES);
    minOldGenSlidingWindow = new MinOldGenSlidingWindow(SLIDING_WINDOW_SIZE_IN_MINS,
        TimeUnit.MINUTES);
    this.nodeStatAggregators = new ArrayList<>();
    for (Metric consumerMetric : consumers) {
      if (consumerMetric != null) {
        this.nodeStatAggregators.add(new NodeStatAggregator(consumerMetric));
      }
    }
    this.topK = HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K;
  }

  public <M extends Metric> HighHeapUsageOldGenRca(final int rcaPeriod,
      final M heap_Used, final M gc_event, final M heap_Max, final List<Metric> consumers) {
    this(rcaPeriod, 1.0, heap_Used, gc_event, heap_Max, consumers);
  }

  @Override
  public ResourceFlowUnit<HotResourceSummary> operate() {
    counter += 1;

    double oldGenHeapUsed = getOldGenUsedOrDefault(Double.NaN);
    int oldGenGCEvent = getFullGcEventsOrDefault(0);
    maxOldGenHeapSize = getMaxOldGenSizeOrDefault(Double.MAX_VALUE);

    long currTimeStamp = this.clock.millis();
    if (!Double.isNaN(oldGenHeapUsed)) {
      LOG.debug(
          "oldGenHeapUsed = {}, oldGenGCEvent = {}, maxOldGenHeapSize = {}",
          oldGenHeapUsed,
          oldGenGCEvent,
          maxOldGenHeapSize);
      gcEventSlidingWindow.next(new SlidingWindowData(currTimeStamp, oldGenGCEvent));
      minOldGenSlidingWindow.next(new SlidingWindowData(currTimeStamp, oldGenHeapUsed));
    }

    //collect node stats from metrics
    for (NodeStatAggregator nodeStatAggregator : this.nodeStatAggregators) {
      nodeStatAggregator.collect(currTimeStamp);
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
        PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
            RcaVerticesMetrics.NUM_OLD_GEN_RCA_TRIGGERED, "", 1);
      } else {
        context = new ResourceContext(Resources.State.HEALTHY);
      }

      //check to see if the value is above lower bound thres
      if (gcEventSlidingWindow.readSum() >= OLD_GEN_GC_THRESHOLD
          && !Double.isNaN(currentMinOldGenUsage)
          && currentMinOldGenUsage / maxOldGenHeapSize > OLD_GEN_USED_THRESHOLD_IN_PERCENTAGE * this.lowerBoundThreshold) {
        summary = new HotResourceSummary(OLD_GEN_HEAP_USAGE,
            OLD_GEN_USED_THRESHOLD_IN_PERCENTAGE, currentMinOldGenUsage / maxOldGenHeapSize, SLIDING_WINDOW_SIZE_IN_MINS * 60);
        addTopConsumers(summary);
      }

      LOG.debug("High Heap Usage RCA Context = " + context.toString());
      return new ResourceFlowUnit<>(this.clock.millis(), context, summary);
    } else {
      // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA
      // state)
      LOG.debug("Empty FlowUnit returned for High Heap Usage RCA");
      return new ResourceFlowUnit<>(this.clock.millis());
    }
  }

  //add top k consumers to summary
  private void addTopConsumers(HotResourceSummary summary) {
    this.nodeStatAggregators.sort(
        (NodeStatAggregator n1, NodeStatAggregator n2) -> Integer.compare(n2.getSum(), n1.getSum())
    );
    for (NodeStatAggregator aggregator : this.nodeStatAggregators) {
      if (aggregator.isEmpty()) {
        continue;
      }
      if (summary.getNestedSummaryList().size() >= topK) {
        break;
      }
      summary.appendNestedSummary(new TopConsumerSummary(aggregator.getName(), aggregator.getSum()));
    }
  }


  /**
   * read top k value from rca.conf
   * @param conf RcaConf object
   */
  @Override
  public void readRcaConf(RcaConf conf) {
    HighHeapUsageOldGenRcaConfig configObj = conf.getHighHeapUsageOldGenRcaConfig();
    topK = configObj.getTopK();
  }

  /**
   * This is a local node RCA which by definition can not be serialize/de-serialized
   * over gRPC.
   */
  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
        + "be required.");
  }
}
