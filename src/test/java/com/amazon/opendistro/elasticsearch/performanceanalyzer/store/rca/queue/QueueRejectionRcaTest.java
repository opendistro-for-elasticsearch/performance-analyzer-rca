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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.queue;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;
import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.TopConsumerSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.queue.QueueRejectionRca;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class QueueRejectionRcaTest {

  private MetricTestHelper threadPool_RejectedReqs;
  private QueueRejectionRca queueRejectionRca;
  private List<String> columnName;

  /**
   * generate flowunit and bind the flowunits it generate to metrics
   */
  private void mockFlowUnits(int writeRejectCnt, int searchRejectCnt) {
    threadPool_RejectedReqs.createTestFlowUnitsWithMultipleRows(columnName,
        Arrays.asList(
            Arrays.asList(ThreadPoolType.WRITE.toString(), String.valueOf(writeRejectCnt)),
            Arrays.asList(ThreadPoolType.SEARCH.toString(), String.valueOf(searchRejectCnt))
        )
    );
  }

  @Before
  public void init() {
    threadPool_RejectedReqs = new MetricTestHelper(5);
    queueRejectionRca = new QueueRejectionRca(1, threadPool_RejectedReqs);
    columnName = Arrays.asList(THREAD_POOL_TYPE.toString(), MetricsDB.MAX);
  }

  @Test
  public void testWriteQueueOnly() {
    ResourceFlowUnit flowUnit;
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

    mockFlowUnits(0, 0);
    queueRejectionRca.setClock(constantClock);
    flowUnit = queueRejectionRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    mockFlowUnits(0, 0);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(3)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    mockFlowUnits(1, 0);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(4)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    mockFlowUnits(1, 0);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(7)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    mockFlowUnits(1, 0);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(10)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());

    Assert.assertTrue(flowUnit.hasResourceSummary());
    Assert.assertTrue(flowUnit.getResourceSummary() instanceof HotResourceSummary);
    HotResourceSummary resourceSummary = (HotResourceSummary) flowUnit.getResourceSummary();
    Assert.assertEquals(1, resourceSummary.getNestedSummaryList().size());
    Assert.assertTrue(resourceSummary.getNestedSummaryList().get(0) instanceof TopConsumerSummary);
    TopConsumerSummary consumerSummary = (TopConsumerSummary) resourceSummary.getNestedSummaryList().get(0);
    Assert.assertEquals(ThreadPoolType.WRITE.toString(), consumerSummary.getName());
    Assert.assertEquals(0.01, 6.0, consumerSummary.getValue());

    mockFlowUnits(0, 0);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(12)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());
  }

  @Test
  public void testWriteAndSearchQueues() {
    ResourceFlowUnit flowUnit;
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

    mockFlowUnits(0, 0);
    queueRejectionRca.setClock(constantClock);
    flowUnit = queueRejectionRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    mockFlowUnits(0, 1);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(3)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    mockFlowUnits(1, 1);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(5)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    mockFlowUnits(1, 1);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(12)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());

    Assert.assertTrue(flowUnit.hasResourceSummary());
    Assert.assertTrue(flowUnit.getResourceSummary() instanceof HotResourceSummary);
    HotResourceSummary resourceSummary = (HotResourceSummary) flowUnit.getResourceSummary();
    Assert.assertEquals(2, resourceSummary.getNestedSummaryList().size());
    Assert.assertTrue(resourceSummary.getNestedSummaryList().get(0) instanceof TopConsumerSummary);
    TopConsumerSummary consumerSummary = (TopConsumerSummary) resourceSummary.getNestedSummaryList().get(0);
    Assert.assertEquals(ThreadPoolType.SEARCH.toString(), consumerSummary.getName());
    Assert.assertEquals(0.01, 9.0, consumerSummary.getValue());
    consumerSummary = (TopConsumerSummary) resourceSummary.getNestedSummaryList().get(1);
    Assert.assertEquals(ThreadPoolType.WRITE.toString(), consumerSummary.getName());
    Assert.assertEquals(0.01, 7.0, consumerSummary.getValue());
  }
}
