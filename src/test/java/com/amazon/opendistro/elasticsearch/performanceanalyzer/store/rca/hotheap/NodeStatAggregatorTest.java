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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.hotheap;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.categories.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.NodeStatAggregator;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class NodeStatAggregatorTest {

  private MetricTestHelper nodeStat;
  private NodeStatAggregator nodeStatAggregator;

  @Before
  public void setup() {
    this.nodeStat = new MetricTestHelper(5);
    this.nodeStatAggregator = new NodeStatAggregator(nodeStat);
  }

  @Test
  public void testCollect() {
    List<String> columnName = Arrays.asList(CommonDimension.INDEX_NAME.toString(), CommonDimension.SHARD_ID.toString(), MetricsDB.MAX);

    nodeStat.createTestFlowUnits(columnName, Arrays.asList("index1", "1", "5"));
    nodeStatAggregator.collect(0);
    Assert.assertEquals(5, nodeStatAggregator.getSum());

    nodeStat.createTestFlowUnits(columnName, Arrays.asList("index1", "2", "3"));
    nodeStatAggregator.collect(TimeUnit.MINUTES.toMillis(3));
    Assert.assertEquals(8, nodeStatAggregator.getSum());

    nodeStat.createTestFlowUnits(columnName, Arrays.asList("index2", "1", "10"));
    nodeStatAggregator.collect(TimeUnit.MINUTES.toMillis(8));
    Assert.assertEquals(18, nodeStatAggregator.getSum());

    nodeStat.createTestFlowUnits(columnName, Arrays.asList("index1", "2", "1"));
    nodeStatAggregator.collect(TimeUnit.MINUTES.toMillis(12));
    Assert.assertEquals(16, nodeStatAggregator.getSum());

    //purge the hash after 30min
    nodeStat.createTestFlowUnits(columnName, Arrays.asList("index2", "2", "10"));
    nodeStatAggregator.collect(TimeUnit.MINUTES.toMillis(32));
    Assert.assertEquals(21, nodeStatAggregator.getSum());
  }
}
