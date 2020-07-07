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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hot_node;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.TopConsumerSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataTypeException;

/**
 * Generic resource type RCA. ideally this RCA can be extended to any resource type
 * and calculate the total resource usage & top consumers.
 */
public class GenericResourceRca extends Rca<ResourceFlowUnit<HotResourceSummary>> {

  private static final Logger LOG = LogManager.getLogger(GenericResourceRca.class);
  private static final int SLIDING_WINDOW_IN_MIN = 10;
  private static final int DEFAULT_TOP_K = 3;
  private static final double DEFAULT_LOWER_BOUND_THRESHOLD = 0.0;
  private final SlidingWindow<SlidingWindowData> slidingWindow;
  private final Metric resourceUsageGroupByConsumer;
  private final int rcaPeriod;
  private int counter;
  private final Resource resource;
  private volatile double threshold;
  private volatile double lowerBoundThreshold;
  private volatile int topK;
  protected Clock clock;

  /**
   * @param rcaPeriod num of rca periods for each evaluation interval
   * @param resource resource type enum
   * @param threshold threshold to identify contented resource
   * @param resourceUsageGroupByConsumer aggregate metric that groups resource
   *                                     metrics by some columns
   * @param <M> Metric base class
   */
  public <M extends Metric> GenericResourceRca(final int rcaPeriod,
      final Resource resource, final double threshold,
      final M resourceUsageGroupByConsumer) {
    super(5);
    this.resourceUsageGroupByConsumer = resourceUsageGroupByConsumer;
    slidingWindow = new SlidingWindow<>(SLIDING_WINDOW_IN_MIN, TimeUnit.MINUTES);
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    this.clock = Clock.systemUTC();
    this.threshold = threshold;
    this.lowerBoundThreshold = DEFAULT_LOWER_BOUND_THRESHOLD;
    this.resource = resource;
    this.topK = DEFAULT_TOP_K;
  }

  /**
   * set the number of top consumers to report
   * This method is thread safe
   * @param topK number of top consumers
   */
  public void setTopK(final int topK) {
    this.topK = Math.max(topK, 0);
  }

  /**
   * set threshold to identify contented resource
   * This method is thread safe
   * @param threshold threshold
   */
  public void setThreshold(final double threshold) {
    this.threshold = threshold;
  }

  /**
   * set lower bound threshold and drop unwanted noisy data
   * if lower than the threshold
   * This method is thread safe
   * @param lowerBoundThreshold lower bound threshold
   */
  public void setLowerBoundThreshold(final double lowerBoundThreshold) {
    this.lowerBoundThreshold = lowerBoundThreshold;
  }

  @Override
  public ResourceFlowUnit<HotResourceSummary> operate() {
    counter += 1;

    for (MetricFlowUnit flowunit : resourceUsageGroupByConsumer.getFlowUnits()) {
      if (flowunit.isEmpty()) {
        continue;
      }
      final Result<Record> result = flowunit.getData();
      if (result == null) {
        continue;
      }
      boolean recordParsingError = false;
      double totalUsage = 0.0;
      for (Record record : result) {
        int fieldSize = record.size();
        if (fieldSize < 2) {
          LOG.error("Field size {} is less than 2, the SQL record has wrong data format", fieldSize);
          recordParsingError = true;
          break;
        }
        try {
          double num = record.getValue(fieldSize - 1, Double.class);
          totalUsage += num;
        }
        catch (DataTypeException de) {
          LOG.error("Fail to data field from SQL record, field index : {}, trace : {}", fieldSize - 1, de.getStackTrace());
          recordParsingError = true;
          break;
        }
      }
      if (!recordParsingError) {
        slidingWindow.next(new SlidingWindowData(this.clock.millis(), totalUsage));
      }
    }

    if (counter == rcaPeriod) {
      ResourceContext context = null;
      HotResourceSummary summary = null;
      // reset the variables
      counter = 0;

      double avgCpuUsage = slidingWindow.readAvg();
      if (!Double.isNaN(avgCpuUsage) && avgCpuUsage > threshold) {
        context = new ResourceContext(Resources.State.CONTENDED);
      } else {
        context = new ResourceContext(Resources.State.HEALTHY);
      }

      //check to see if the value is above lower bound thres
      if (!Double.isNaN(avgCpuUsage) && avgCpuUsage >= lowerBoundThreshold) {
        summary = new HotResourceSummary(this.resource, threshold,
            avgCpuUsage, SLIDING_WINDOW_IN_MIN * 60);
        addTopConsumerSummary(summary);
      }
      return new ResourceFlowUnit<>(clock.millis(), context, summary);
    } else {
      // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA state)
      return new ResourceFlowUnit<>(clock.millis());
    }
  }

  // build top k consumers list and add the list to summary.
  // The list of record is sorted in desc order by AggregateMetric wo we simply
  // pick the top k from the list.
  private void addTopConsumerSummary(HotResourceSummary summary) {
    List<MetricFlowUnit> flowUnits = resourceUsageGroupByConsumer.getFlowUnits();
    if (!flowUnits.isEmpty() && flowUnits.get(0).getData() != null) {
      for (Record record : flowUnits.get(0).getData()) {
        if (summary.getNestedSummaryList().size() >= this.topK) {
          break;
        }
        int fieldSize = record.size();
        try {
          double num = record.getValue(fieldSize - 1, Double.class);
          String name = record.getValue(0, String.class);
          summary.appendNestedSummary(new TopConsumerSummary(name, num));
        }
        catch (DataTypeException de) {
          LOG.error("Fail to read some field from SQL record, trace : {}", de.getStackTrace());
          break;
        }
      }
    }
  }

  // The intention of adding this RCA to collect/aggregate cpu usage on this node
  // the flowunit coming out of this RCA is supposed to be ingested by another
  // node level RCA. So we don't need to worry about serialization/de-serialization
  // over gRPC here
  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    LOG.error("rca: {} is not supposed to be received from wire", this.getClass().getSimpleName());
  }
}
