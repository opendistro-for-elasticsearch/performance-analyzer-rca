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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.threadpool;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;
import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.threadpool.QueueRejectionRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
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
  public void init() throws Exception {
    threadPool_RejectedReqs = new MetricTestHelper(5);
    queueRejectionRca = new QueueRejectionRca(1, threadPool_RejectedReqs);
    columnName = Arrays.asList(THREAD_POOL_TYPE.toString(), MetricsDB.MAX);

    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    clusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(
        new ClusterDetailsEventProcessor.NodeDetails(
            AllMetrics.NodeRole.DATA,
            "node1",
            "127.0.0.1",
            false
        )
    ));
    AppContext appContext = new AppContext();
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

    queueRejectionRca.setAppContext(appContext);
  }

  @Test
  public void testWriteQueueOnly() {
    ResourceFlowUnit<HotNodeSummary> flowUnit;
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
    HotNodeSummary nodeSummary = flowUnit.getSummary();
    Assert.assertEquals(1, nodeSummary.getNestedSummaryList().size());
    Assert.assertEquals(1, nodeSummary.getHotResourceSummaryList().size());
    HotResourceSummary resourceSummary = nodeSummary.getHotResourceSummaryList().get(0);
    Assert.assertEquals(ResourceUtil.WRITE_QUEUE_REJECTION, resourceSummary.getResource());
    Assert.assertEquals(0.01, 6.0, resourceSummary.getValue());

    mockFlowUnits(0, 0);
    queueRejectionRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(12)));
    flowUnit = queueRejectionRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
  }

  @Test
  public void testWriteAndSearchQueues() {
    ResourceFlowUnit<HotNodeSummary> flowUnit;
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
    HotNodeSummary nodeSummary = flowUnit.getSummary();
    Assert.assertEquals(2, nodeSummary.getNestedSummaryList().size());
    Assert.assertEquals(2, nodeSummary.getHotResourceSummaryList().size());
    HotResourceSummary resourceSummary = nodeSummary.getHotResourceSummaryList().get(1);
    Assert.assertEquals(ResourceUtil.SEARCH_QUEUE_REJECTION, resourceSummary.getResource());
    Assert.assertEquals(0.01, 9.0, resourceSummary.getValue());
    resourceSummary = nodeSummary.getHotResourceSummaryList().get(0);
    Assert.assertEquals(ResourceUtil.WRITE_QUEUE_REJECTION, resourceSummary.getResource());
    Assert.assertEquals(0.01, 7.0, resourceSummary.getValue());
  }
}
