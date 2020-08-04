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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.collector;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapValue.HEAP_MAX;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.NodeConfigFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Max_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.flow_units.MetricFlowUnitTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class NodeConfigCollectorTest {

  private ThreadPool_QueueCapacity threadPool_QueueCapacity;
  private Cache_Max_Size cacheMaxSize;
  private Heap_Max heapMax;
  private NodeConfigCollector nodeConfigCollector;

  @Before
  public void init() {
    threadPool_QueueCapacity = new ThreadPool_QueueCapacity();
    cacheMaxSize = new Cache_Max_Size(5);
    heapMax = new Heap_Max(5);
    nodeConfigCollector = new NodeConfigCollector(1, threadPool_QueueCapacity, cacheMaxSize, heapMax);

    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    ClusterDetailsEventProcessor.NodeDetails node1 =
        new ClusterDetailsEventProcessor.NodeDetails(AllMetrics.NodeRole.DATA, "node1", "127.0.0.0", false);
    clusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(node1));
    AppContext appContext = new AppContext();
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);
    nodeConfigCollector.setAppContext(appContext);
  }

  /**
   * generate flowunit and bind the flowunits it generate to metrics
   */
  @SuppressWarnings("unchecked")
  private void mockQueueFlowUnits(int writeQueueCapacity, int searchQueueCapacity) {
    MetricFlowUnit flowUnit = MetricFlowUnitTestHelper.createFlowUnit(
        Arrays.asList(THREAD_POOL_TYPE.toString(), MetricsDB.MAX),
        Arrays.asList(ThreadPoolType.WRITE.toString(), String.valueOf(writeQueueCapacity)),
        Arrays.asList(ThreadPoolType.SEARCH.toString(), String.valueOf(searchQueueCapacity))
    );
    threadPool_QueueCapacity.setLocalFlowUnit(flowUnit);
  }

  /**
   * generate flowunit and bind the flowunits it generate to metrics
   */
  @SuppressWarnings("unchecked")
  private void mockHeapMaxSizeFlowUnits(int heapMaxSize) {
    MetricFlowUnit flowUnit =
        MetricFlowUnitTestHelper.createFlowUnit(
            Arrays.asList(MEM_TYPE.toString(), MetricsDB.MAX),
            Arrays.asList(HEAP_MAX.toString(), String.valueOf(heapMaxSize)));
    heapMax.setLocalFlowUnit(flowUnit);
  }

  @Test
  public void testQueueCapacityMetricNotExist() {
    threadPool_QueueCapacity.setLocalFlowUnit(MetricFlowUnit.generic());
    NodeConfigFlowUnit flowUnit = nodeConfigCollector.operate();
    Assert.assertTrue(flowUnit.isEmpty());
    Assert.assertFalse(flowUnit.hasConfig(ResourceUtil.SEARCH_QUEUE_CAPACITY));
    Assert.assertFalse(flowUnit.hasConfig(ResourceUtil.WRITE_QUEUE_CAPACITY));
  }

  @Test
  public void testCacheMaxSizeMetricNotExist() {
    cacheMaxSize.setLocalFlowUnit(MetricFlowUnit.generic());
    NodeConfigFlowUnit flowUnit = nodeConfigCollector.operate();
    Assert.assertTrue(flowUnit.isEmpty());
    Assert.assertFalse(flowUnit.hasConfig(ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE));
    Assert.assertFalse(flowUnit.hasConfig(ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE));
  }

  @Test
  public void testHeapMaxSizeMetricNotExist() {
    heapMax.setLocalFlowUnit(MetricFlowUnit.generic());
    NodeConfigFlowUnit flowUnit = nodeConfigCollector.operate();
    Assert.assertTrue(flowUnit.isEmpty());
    Assert.assertFalse(flowUnit.hasConfig(ResourceUtil.HEAP_MAX_SIZE));
  }

  @Test
  public void testQueueCapacityCollection() {
    mockQueueFlowUnits(100, 200);
    NodeConfigFlowUnit flowUnit = nodeConfigCollector.operate();
    Assert.assertFalse(flowUnit.isEmpty());
    Assert.assertTrue(flowUnit.hasConfig(ResourceUtil.SEARCH_QUEUE_CAPACITY));
    Assert.assertEquals(200, flowUnit.readConfig(ResourceUtil.SEARCH_QUEUE_CAPACITY), 0.01);
    Assert.assertTrue(flowUnit.hasConfig(ResourceUtil.WRITE_QUEUE_CAPACITY));
    Assert.assertEquals(100, flowUnit.readConfig(ResourceUtil.WRITE_QUEUE_CAPACITY), 0.01);
  }

  @Test
  public void testHeapMaxSizeCollection() {
    mockHeapMaxSizeFlowUnits(10000);
    NodeConfigFlowUnit flowUnit = nodeConfigCollector.operate();
    Assert.assertFalse(flowUnit.isEmpty());
    Assert.assertTrue(flowUnit.hasConfig(ResourceUtil.HEAP_MAX_SIZE));
    Assert.assertEquals(10000, flowUnit.readConfig(ResourceUtil.HEAP_MAX_SIZE), 0.01);
  }
}
