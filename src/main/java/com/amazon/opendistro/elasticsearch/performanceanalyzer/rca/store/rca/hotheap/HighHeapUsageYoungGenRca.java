/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.OLD_GEN;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_FULL_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_YOUNG_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.FULL_GC_PAUSE_TIME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.MINOR_GC_PAUSE_TIME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.YOUNG_GEN_PROMOTION_RATE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCInfoDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageYoungGenRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Type;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaVerticesMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;

/**
 * This RCA is used to decide whether the young generation in CMS garbage collector is healthy or
 * not. It subscribes two different metrics : heap_usage and gc_collection_time. For
 * gc_collection_time we simply measure its moving average. And we use heap_used to measure the
 * increase in old gen usage for each sampling to decide the amount of objects being promoted to old
 * gen during the last time interval and then use it to calculate its moving average. If both the
 * promotion rate and young gen GC time reach the threshold, this node is marked as unhealthy.
 */
public class HighHeapUsageYoungGenRca extends Rca<ResourceFlowUnit<HotResourceSummary>> {

  private static final Logger LOG = LogManager.getLogger(HighHeapUsageYoungGenRca.class);
  private static final String FULL_GC_TIME_TOO_HIGH = "fullGcTimeTooHigh";
  private static final String PROMOTION_RATE_TOO_HIGH = "promotionRateTooHigh";
  private static final String YOUNG_GC_TIME_TOO_HIGH = "youngGcTimeTooHigh";
  private static final String PREMATURE_PROMOTION_TOO_HIGH = "prematurePromotionTooHigh";
  private static final String CMS_COLLECTOR = "ConcurrentMarkSweep";

  private static final int PROMOTION_RATE_SLIDING_WINDOW_IN_MINS = 10;
  private static final double FULL_GC_TIME_THRES_MS = 10 * 1_000;
  private static final double CONVERT_BYTES_TO_MEGABYTES = Math.pow(1024, 2);
  private final Metric heap_Used;
  private final Metric gc_Collection_Time;
  private final Metric gc_Collection_Event;
  private final Metric gc_type;
  // the amount of RCA period this RCA needs to run before sending out a flowunit
  private final int rcaPeriod;
  // The lower bound threshold in percentage to decide whether to send out summary.
  // e.g. if lowerBoundThreshold = 0.2, then we only send out summary if value > 0.2*threshold
  private final double lowerBoundThreshold;
  private int counter;
  private final SlidingWindow<SlidingWindowData> minorGcTimeDeque;
  private final SlidingWindow<SlidingWindowData> fullGcTimeDeque;
  private final SlidingWindow<SlidingWindowData> promotionRateDeque;
  private final SlidingWindow<SlidingWindowData> garbagePromotedDeque;
  //promotion rate in mb/s
  private int promotionRateThreshold;
  //young gc time in ms per second
  private int youngGenGcTimeThreshold;
  // garbage promotion percentage threshold
  private double garbagePromotionPctThreshold;
  // variables used to roughly compute garbage promotion percentage
  private double youngGenPromotedBytes = 0d;
  private double maxOldGen = 0d;
  private double prevOldGen = 0d;
  protected Clock clock;

  public <M extends Metric> HighHeapUsageYoungGenRca(final int rcaPeriod,
                                                     final double lowerBoundThreshold,
                                                     final M heap_Used,
                                                     final M gc_Collection_Time,
                                                     final M gc_Collection_Event,
                                                     final M gc_Type) {
    super(5);
    this.clock = Clock.systemUTC();
    this.heap_Used = heap_Used;
    this.gc_Collection_Time = gc_Collection_Time;
    this.gc_Collection_Event = gc_Collection_Event;
    this.gc_type = gc_Type;
    this.rcaPeriod = rcaPeriod;
    this.lowerBoundThreshold = (lowerBoundThreshold >= 0 && lowerBoundThreshold <= 1.0)
        ? lowerBoundThreshold : 1.0;
    this.counter = 0;
    this.minorGcTimeDeque = new SlidingWindow<>(PROMOTION_RATE_SLIDING_WINDOW_IN_MINS, TimeUnit.MINUTES);
    this.fullGcTimeDeque = new SlidingWindow<>(PROMOTION_RATE_SLIDING_WINDOW_IN_MINS, TimeUnit.MINUTES);
    this.promotionRateThreshold = HighHeapUsageYoungGenRcaConfig.DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC;
    this.youngGenGcTimeThreshold = HighHeapUsageYoungGenRcaConfig.DEFAULT_YOUNG_GEN_GC_TIME_THRESHOLD_IN_MS_PER_SEC;

    this.garbagePromotedDeque = new SlidingWindow<>(PROMOTION_RATE_SLIDING_WINDOW_IN_MINS, TimeUnit.MINUTES);
    this.promotionRateDeque = new SlidingWindow<SlidingWindowData>(PROMOTION_RATE_SLIDING_WINDOW_IN_MINS, TimeUnit.MINUTES) {
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
      protected void add(SlidingWindowData e) {
        if (!windowDeque.isEmpty() && e.getValue() > windowDeque.peekFirst().getValue()) {
          sum += (e.getValue() - windowDeque.peekFirst().getValue());
        }
      }

      @Override
      protected void remove(SlidingWindowData e) {
        if (!windowDeque.isEmpty() && e.getValue() < windowDeque.peekLast().getValue()) {
          sum -= (windowDeque.peekLast().getValue() - e.getValue());
        }
      }
    };
  }

  public <M extends Metric> HighHeapUsageYoungGenRca(final int rcaPeriod,
      final M heap_Used, final M gc_Collection_Time, final M gc_Collection_Event, final M gcType) {
    this(rcaPeriod, 1.0, heap_Used, gc_Collection_Time, gc_Collection_Event, gcType);
  }

  private boolean fullGcTimeTooHigh(double avgFullGcTime) {
    return (!Double.isNaN(avgFullGcTime) && avgFullGcTime > FULL_GC_TIME_THRES_MS);
  }

  private boolean promotionRateTooHigh(double avgPromotionRate, double modifier) {
    return (!Double.isNaN(avgPromotionRate) && avgPromotionRate > promotionRateThreshold * modifier);
  }

  private boolean youngGcTimeTooHigh(double avgYoungGCTime) {
    return (!Double.isNaN(avgYoungGCTime) && avgYoungGCTime > youngGenGcTimeThreshold);
  }

  private boolean prematurePromotionTooHigh(double avgGarbagePromoted) {
    return (!Double.isNaN(avgGarbagePromoted)
        && avgGarbagePromoted <= 1
        && avgGarbagePromoted > garbagePromotionPctThreshold);
  }

  private double getFollowerCheckTimeoutMs() {
    return FULL_GC_TIME_THRES_MS;
  }

  /**
   * Creates and returns a ResourceFlowUnit that reflects the health of this RCA
   *
   * @param avgPromotionRate the average rate of promotion of objects from the young generation to
   *                         the old generation in MB/s
   * @param avgYoungGCTime the average amount of time spent on GC in ms
   * @param avgGarbagePromoted the average amount of objects which were promoted to the old generation
   *                           then collected from the old generation shortly after. This is a percentage
   *                           from 0.0 to 1.0
   * @param avgFullGCTime The average amount of time spend of full GC in ms
   * @return A ResourceFlowUnit encapsulating the health of this RCA
   */
  private ResourceFlowUnit<HotResourceSummary> computeFlowUnit(double avgPromotionRate,
                                                               double avgYoungGCTime,
                                                               double avgGarbagePromoted,
                                                               double avgFullGCTime) {
    LOG.debug("computing avgPromotionRate = {} , avgGCTime = {}, avgGarbagePromoted = {}, avgFullGcTime = {},",
        avgPromotionRate, avgYoungGCTime, avgGarbagePromoted, avgFullGCTime);
    ResourceContext context = new ResourceContext(State.UNHEALTHY);
    HotResourceSummary summary = null;
    boolean unhealthy = true;

    // Check if the RCA is unhealthy
    if (fullGcTimeTooHigh(avgFullGCTime)) {
      LOG.info("Average full GC time is unhealthy " + avgFullGCTime);
      summary = new HotResourceSummary(FULL_GC_PAUSE_TIME, getFollowerCheckTimeoutMs(), avgFullGCTime,
          PROMOTION_RATE_SLIDING_WINDOW_IN_MINS * 60);
      PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
          RcaVerticesMetrics.YOUNG_GEN_RCA_NAMED_COUNT, FULL_GC_TIME_TOO_HIGH, 1);
    } else if (promotionRateTooHigh(avgPromotionRate, this.lowerBoundThreshold)) {
      //check to see if the value is above lower bound thres
      summary = new HotResourceSummary(YOUNG_GEN_PROMOTION_RATE,
          promotionRateThreshold * this.lowerBoundThreshold, avgPromotionRate,
          PROMOTION_RATE_SLIDING_WINDOW_IN_MINS * 60);
      PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
          RcaVerticesMetrics.YOUNG_GEN_RCA_NAMED_COUNT, PROMOTION_RATE_TOO_HIGH, 1);
    } else if (youngGcTimeTooHigh(avgYoungGCTime)) {
      summary = new HotResourceSummary(MINOR_GC_PAUSE_TIME, youngGenGcTimeThreshold, avgYoungGCTime,
          PROMOTION_RATE_SLIDING_WINDOW_IN_MINS * 60);
      PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
          RcaVerticesMetrics.YOUNG_GEN_RCA_NAMED_COUNT, YOUNG_GC_TIME_TOO_HIGH, 1);
    } else if (prematurePromotionTooHigh(avgGarbagePromoted)) {
      summary = new HotResourceSummary(YOUNG_GEN_PROMOTION_RATE, garbagePromotionPctThreshold,
          avgGarbagePromoted, PROMOTION_RATE_SLIDING_WINDOW_IN_MINS * 60);
      PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
          RcaVerticesMetrics.YOUNG_GEN_RCA_NAMED_COUNT, PREMATURE_PROMOTION_TOO_HIGH, 1);
    } else {
      unhealthy = false;
      context = new ResourceContext(State.HEALTHY);
    }

    if (unhealthy) {
      LOG.debug("avgPromotionRate = {} , avgGCTime = {}, avgGarbagePromoted = {}, avgFullGcTime = {},",
          avgPromotionRate, avgYoungGCTime, avgGarbagePromoted, avgFullGCTime);
      PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
          RcaVerticesMetrics.NUM_YOUNG_GEN_RCA_TRIGGERED, "", 1);
    }

    return new ResourceFlowUnit<>(this.clock.millis(), context, summary);
  }

  /**
   * computes the amount of data promoted and reclaimed by the garbage collector
   * @param currOldGen the current occupancy of the old generation in bytes
   * @param currTimeStamp the current timestamp in UNIX epoch milliseconds
   */
  private void computePromotionHealth(double currOldGen, double fullGcCount, long currTimeStamp) {
    if (currOldGen > maxOldGen) {
      maxOldGen = currOldGen;
    }
    double promoted = currOldGen - prevOldGen;
    if (promoted >= 0) {
      youngGenPromotedBytes += promoted;
    } else if (fullGcCount > 0 && youngGenPromotedBytes > 0) {
      double reclaimed = maxOldGen - currOldGen;
      double garbageReclaimedPct = reclaimed / youngGenPromotedBytes;
      if (garbageReclaimedPct <= 1 && garbageReclaimedPct >= 0) {
        garbagePromotedDeque.next(new SlidingWindowData(currTimeStamp, garbageReclaimedPct));
      }
      // Reset variables
      youngGenPromotedBytes = 0;
      maxOldGen = currOldGen;
    }
    prevOldGen = currOldGen;
  }

  protected boolean isCollectorCMS() {
    if (gc_type == null) {
      throw new IllegalStateException("RCA: " + this.name() + "was not configured in the graph to "
          + "take GC_Type as a metric. Please check the analysis graph!");
    }

    final List<MetricFlowUnit> gcTypeFlowUnits = gc_type.getFlowUnits();
    Field<String> memTypeField = GCInfoDimension.MEMORY_POOL.getField();
    Field<String> collectorField = GCInfoDimension.COLLECTOR_NAME.getField();
    for (MetricFlowUnit gcTypeFlowUnit : gcTypeFlowUnits) {
      if (gcTypeFlowUnit.isEmpty()) {
        continue;
      }

      Result<Record> records = gcTypeFlowUnit.getData();
      for (final Record record : records) {
        final String memType = record.get(memTypeField);
        if (OLD_GEN.toString().equals(memType)) {
          return CMS_COLLECTOR.equals(record.get(collectorField));
        }
      }
    }

    // We want to return true here because we don't want to hold up evaluation of RCAs due to
    // transient metric delays. We don't want to tune the JVM only when we know for sure that the
    // collector is not CMS, in all other cases, give JVM RCAs the benefit of the doubt.
    return true;
  }

  @Override
  public ResourceFlowUnit<HotResourceSummary> operate() {
    if (!isCollectorCMS()) {
      // return an empty flow unit. We don't want to tune the JVM when the collector is not CMS.
      return new ResourceFlowUnit<>(System.currentTimeMillis());
    }

    long currTimeStamp = this.clock.millis();
    counter += 1;

    LOG.debug("HighHeapUsageYoungGenRca getting collection event flow units");
    double fullGcCount = 0;
    for (MetricFlowUnit metricFU : gc_Collection_Event.getFlowUnits()) {
      if (metricFU.isEmpty()) {
        continue;
      }
      fullGcCount = SQLParsingUtil.readDataFromSqlResult(metricFU.getData(),
          MEM_TYPE.getField(), OLD_GEN.toString(), MetricsDB.MAX);
      if (Double.isNaN(fullGcCount)) {
        fullGcCount = 0;
        LOG.error("Failed to parse metric in FlowUnit from {}", gc_Collection_Event.getClass().getName());
      }
    }

    LOG.debug("HighHeapUsageYoungGenRca getting heap used flow units");
    //parsing flowunits from heap_used and push them into sliding window
    for (MetricFlowUnit metricFU : heap_Used.getFlowUnits()) {
      if (metricFU.isEmpty()) {
        continue;
      }
      double oldGenHeapUsed = SQLParsingUtil.readDataFromSqlResult(metricFU.getData(),
          MEM_TYPE.getField(), OLD_GEN.toString(), MetricsDB.MAX);
      if (!Double.isNaN(oldGenHeapUsed)) {
        promotionRateDeque.next(new SlidingWindowData(currTimeStamp, oldGenHeapUsed / CONVERT_BYTES_TO_MEGABYTES));
        computePromotionHealth(oldGenHeapUsed, fullGcCount, currTimeStamp);
      }
      else {
        LOG.error("Failed to parse metric in FlowUnit from {}", heap_Used.getClass().getName());
      }
    }

    LOG.debug("HighHeapUsageYoungGenRca getting collection time flow units");
    //parsing flowunits from gc_Collection_Time and push them into sliding window
    for (MetricFlowUnit metricFU : gc_Collection_Time.getFlowUnits()) {
      if (metricFU.isEmpty()) {
        continue;
      }
      double totYoungGCTime = SQLParsingUtil.readDataFromSqlResult(metricFU.getData(),
          MEM_TYPE.getField(), TOT_YOUNG_GC.toString(), MetricsDB.MAX);
      if (!Double.isNaN(totYoungGCTime)) {
        minorGcTimeDeque.next(new SlidingWindowData(currTimeStamp, totYoungGCTime));
      }
      double totFullGCTime = SQLParsingUtil.readDataFromSqlResult(metricFU.getData(),
          MEM_TYPE.getField(), TOT_FULL_GC.toString(), MetricsDB.MAX);
      if (!Double.isNaN(totFullGCTime)) {
        fullGcTimeDeque.next(new SlidingWindowData(currTimeStamp, totFullGCTime));
      }
      else {
        LOG.error("Failed to parse metric in FlowUnit from {}", gc_Collection_Time.getClass().getName());
      }
    }

    if (counter == rcaPeriod) {
      LOG.debug("HighHeapUsageYoungGenRca computing data...");
      counter = 0;
      double avgPromotionRate = promotionRateDeque.readAvg(TimeUnit.SECONDS);
      double avgYoungGCTime = minorGcTimeDeque.readAvg(TimeUnit.SECONDS);
      double avgGarbagePromoted = garbagePromotedDeque.readAvg();
      double avgFullGCTime = fullGcTimeDeque.readAvg();
      return computeFlowUnit(avgPromotionRate, avgYoungGCTime, avgGarbagePromoted, avgFullGCTime);
    } else {
      // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA state)
      LOG.debug("RCA: Empty FlowUnit returned for Young Gen RCA");
      return new ResourceFlowUnit<>(this.clock.millis());
    }
  }

  /**
   * read threshold values from rca.conf
   * @param conf RcaConf object
   */
  @Override
  public void readRcaConf(RcaConf conf) {
    HighHeapUsageYoungGenRcaConfig configObj = conf.getHighHeapUsageYoungGenRcaConfig();
    promotionRateThreshold = configObj.getPromotionRateThreshold();
    youngGenGcTimeThreshold = configObj.getYoungGenGcTimeThreshold();
    garbagePromotionPctThreshold = configObj.getGarbagePromotionPctThreshold();
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
