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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.NodeConfigFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigClusterCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class NodeConfigClusterCollectorTest {

  private NodeConfigCollector collector;
  private NodeConfigClusterCollector clusterCollector;
  private RcaTestHelper<HotResourceSummary> observer;

  @Before
  public void init() {
    collector = new NodeConfigCollector(1, null);
    clusterCollector = new NodeConfigClusterCollector(collector);
    observer = new RcaTestHelper<>();
    AppContext appContext = new AppContext();
    clusterCollector.setAppContext(appContext);
    observer.setAppContext(appContext);
  }

  @Test
  public void testCollections() {
    NodeKey nodeKey1 = new NodeKey("node1", "127.0.0.1");
    NodeKey nodeKey2 = new NodeKey("node2", "127.0.0.2");
    NodeConfigFlowUnit flowUnit = new NodeConfigFlowUnit(0, nodeKey1);
    flowUnit.addConfig(ResourceUtil.WRITE_QUEUE_CAPACITY, 100);
    collector.setLocalFlowUnit(flowUnit);
    clusterCollector.operate();
    double val1 = observer.readConfig(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY);
    Assert.assertEquals(100, val1, 0.01);
    double val2 = observer.readConfig(nodeKey1, ResourceUtil.SEARCH_QUEUE_CAPACITY);
    Assert.assertTrue(Double.isNaN(val2));
    double val3 = observer.readConfig(nodeKey2, ResourceUtil.SEARCH_QUEUE_CAPACITY);
    Assert.assertTrue(Double.isNaN(val3));

    flowUnit = new NodeConfigFlowUnit(0, nodeKey1);
    flowUnit.addConfig(ResourceUtil.SEARCH_QUEUE_CAPACITY, 500);
    collector.setLocalFlowUnit(flowUnit);
    clusterCollector.operate();
    val1 = observer.readConfig(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY);
    Assert.assertEquals(100, val1, 0.01);
    val2 = observer.readConfig(nodeKey1, ResourceUtil.SEARCH_QUEUE_CAPACITY);
    Assert.assertEquals(500, val2, 0.01);

    flowUnit = new NodeConfigFlowUnit(0, nodeKey1);
    flowUnit.addConfig(ResourceUtil.WRITE_QUEUE_CAPACITY, 10);
    collector.setLocalFlowUnit(flowUnit);
    clusterCollector.operate();
    val1 = observer.readConfig(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY);
    Assert.assertEquals(10, val1, 0.01);
    val2 = observer.readConfig(nodeKey1, ResourceUtil.SEARCH_QUEUE_CAPACITY);
    Assert.assertEquals(500, val2, 0.01);

    flowUnit = new NodeConfigFlowUnit(0, nodeKey2);
    flowUnit.addConfig(ResourceUtil.WRITE_QUEUE_CAPACITY, 80);
    flowUnit.addConfig(ResourceUtil.SEARCH_QUEUE_CAPACITY, 180);
    collector.setLocalFlowUnit(flowUnit);
    clusterCollector.operate();
    val1 = observer.readConfig(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY);
    Assert.assertEquals(10, val1, 0.01);
    val2 = observer.readConfig(nodeKey1, ResourceUtil.SEARCH_QUEUE_CAPACITY);
    Assert.assertEquals(500, val2, 0.01);
    val1 = observer.readConfig(nodeKey2, ResourceUtil.WRITE_QUEUE_CAPACITY);
    Assert.assertEquals(80, val1, 0.01);
    val2 = observer.readConfig(nodeKey2, ResourceUtil.SEARCH_QUEUE_CAPACITY);
    Assert.assertEquals(180, val2, 0.01);
  }

}