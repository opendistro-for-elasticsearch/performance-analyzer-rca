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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.queue;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ThreadPoolEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.TopConsumerSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This RCA reads ThreadPool_RejectionReqs from upstream metrics and it keeps track of
 * the amount of the time period(tp) when requests in the queues are continuously being rejected.
 * This RCA is marked as unhealthy if tp we find in write/search queue is above the threshold.
 */
public class QueueRejectionRca extends Rca<ResourceFlowUnit> {
  private static final Logger LOG = LogManager.getLogger(QueueRejectionRca.class);
  private static final int REJECTION_TIME_PERIOD_IN_SECONDS = 300;
  private final Metric threadPool_RejectedReqs;
  private final int rcaPeriod;
  private final List<QueueRejectedReqsWrapper> queueRejectedReqsWrappers;
  private final ResourceType resourceType;
  private int counter;
  protected Clock clock;

  public <M extends Metric> QueueRejectionRca(final int rcaPeriod, M threadPool_RejectedReqs) {
    super(5);
    this.threadPool_RejectedReqs = threadPool_RejectedReqs;
    this.rcaPeriod = rcaPeriod;
    resourceType = ResourceType.newBuilder().setThreadpool(ThreadPoolEnum.THREADPOOL_REJECTED_REQS).build();
    counter = 0;
    clock = Clock.systemUTC();
    queueRejectedReqsWrappers = new ArrayList<QueueRejectedReqsWrapper>() {
      {
        add(new QueueRejectedReqsWrapper(ThreadPoolType.WRITE));
        add(new QueueRejectedReqsWrapper(ThreadPoolType.SEARCH));
      }
    };
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @Override
  public ResourceFlowUnit operate() {
    counter += 1;
    long currTimestamp = clock.millis();
    if (counter == rcaPeriod) {
      counter = 0;
      List<TopConsumerSummary> threadpoolSummaries = new ArrayList<>();
      for (QueueRejectedReqsWrapper wrapper : queueRejectedReqsWrappers) {
        wrapper.collect(threadPool_RejectedReqs, currTimestamp);
        // if we've see thread pool rejection in the last 5 mins, the thread pool is considered as contended
        if (wrapper.hasRejection) {
          long timePeriod = TimeUnit.MILLISECONDS.toSeconds(currTimestamp - wrapper.getRejectionTimestamp());
          if (timePeriod >= REJECTION_TIME_PERIOD_IN_SECONDS) {
            threadpoolSummaries.add(new TopConsumerSummary(wrapper.getThreadPool().toString(), timePeriod));
          }
        }
      }
      ResourceContext context = null;
      HotResourceSummary summary = null;
      if (threadpoolSummaries.isEmpty()) {
        context = new ResourceContext(Resources.State.HEALTHY);
      }
      else {
        context = new ResourceContext(Resources.State.UNHEALTHY);
        summary = new HotResourceSummary(this.resourceType,
            0, threadpoolSummaries.size(), REJECTION_TIME_PERIOD_IN_SECONDS);
        threadpoolSummaries.sort(
            (TopConsumerSummary t1, TopConsumerSummary t2) -> Double.compare(t2.getValue(), t1.getValue())
        );
        summary.addNestedSummaryList(threadpoolSummaries);
      }
      return new ResourceFlowUnit(currTimestamp, context, summary);
    }
    else {
      return new ResourceFlowUnit(currTimestamp);
    }
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
        + "be required.");
  }

  private static class QueueRejectedReqsWrapper {
    private ThreadPoolType threadPool;
    private boolean hasRejection;
    private long rejectionTimestamp;

    public QueueRejectedReqsWrapper(final ThreadPoolType threadPool) {
      this.threadPool = threadPool;
      hasRejection = false;
      rejectionTimestamp = 0;
    }

    public ThreadPoolType getThreadPool() {
      return threadPool;
    }

    public long getRejectionTimestamp() {
      return rejectionTimestamp;
    }

    public void collect(final Metric rejectMetric, final long currTimestamp) {
      for (MetricFlowUnit flowUnit : rejectMetric.getFlowUnits()) {
        if (flowUnit.isEmpty()) {
          continue;
        }
        double rejectCnt = SQLParsingUtil.readDataFromSqlResult(flowUnit.getData(),
            THREAD_POOL_TYPE.getField(), threadPool.toString(), MetricsDB.MAX);
        if (!Double.isNaN(rejectCnt)) {
          if (rejectCnt > 0) {
            if (!hasRejection) {
              rejectionTimestamp = currTimestamp;
            }
            hasRejection = true;
          }
          else {
            hasRejection = false;
          }
        }
        else {
          LOG.error("Failed to parse metric from threadpool {}", threadPool.toString());
        }
      }
    }
  }
}


