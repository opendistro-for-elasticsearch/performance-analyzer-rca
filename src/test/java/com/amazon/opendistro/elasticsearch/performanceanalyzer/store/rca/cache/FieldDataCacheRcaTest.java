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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.cache;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsDerivedDimension.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsDerivedDimension.SHARD_ID;
import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.FieldDataCacheRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
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
public class FieldDataCacheRcaTest {

    private MetricTestHelper fieldDataCacheEvictions;
    private MetricTestHelper fieldDataCacheWeight;
    private FieldDataCacheRca fieldDataCacheRca;
    private List<String> columnName;

    @Before
    public void init() throws Exception {
        fieldDataCacheEvictions = new MetricTestHelper(5);
        fieldDataCacheWeight = new MetricTestHelper(5);
        fieldDataCacheRca = new FieldDataCacheRca(1, fieldDataCacheEvictions, fieldDataCacheWeight);
        columnName = Arrays.asList(INDEX_NAME.toString(), SHARD_ID.toString(), MetricsDB.SUM, MetricsDB.MAX);
        ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
        clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
        clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
    }

    /**
     * generate flowunit and bind the flowunit to metrics, sample record:
     *
     * <p>Eg:| IndexName | ShardID | SUM | AVG | MIN | MAX |
     *      -------------------------------------------------
     *       | .kibana_1 | 0       | 15.0 | 8.0 | 2.0 | 9.0 |
     *
     */
    private void mockFlowUnits(int cacheEvictionCnt, double cacheWeight) {
        fieldDataCacheEvictions.createTestFlowUnits(columnName,
                Arrays.asList("index_1", "0", String.valueOf(cacheEvictionCnt), String.valueOf(cacheEvictionCnt)));
        fieldDataCacheWeight.createTestFlowUnits(columnName,
                Arrays.asList("index_1", "0", String.valueOf(cacheWeight), String.valueOf(cacheWeight)));
    }

    @Test
    public void testFieldDataCache() {
        ResourceFlowUnit<HotNodeSummary> flowUnit;
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

        // TimeWindow 41of size 300sec
        mockFlowUnits(0, 1.0);
        fieldDataCacheRca.setClock(constantClock);
        flowUnit = fieldDataCacheRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        mockFlowUnits(0, 1.0);
        fieldDataCacheRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(3)));
        flowUnit = fieldDataCacheRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        mockFlowUnits(1, 1.0);
        fieldDataCacheRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(4)));
        flowUnit = fieldDataCacheRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        // TimeWindow 2 of size 300sec
        mockFlowUnits(1, 7.0);
        fieldDataCacheRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(7)));
        flowUnit = fieldDataCacheRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        mockFlowUnits(1, 1.0);
        fieldDataCacheRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(10)));
        flowUnit = fieldDataCacheRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        // TimeWindow 3 of size 300sec
        mockFlowUnits(1, 7.0);
        fieldDataCacheRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(12)));
        flowUnit = fieldDataCacheRca.operate();
        Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());

        Assert.assertTrue(flowUnit.hasResourceSummary());
        HotNodeSummary nodeSummary = flowUnit.getSummary();
        Assert.assertEquals(1, nodeSummary.getNestedSummaryList().size());
        Assert.assertEquals(1, nodeSummary.getHotResourceSummaryList().size());
        HotResourceSummary resourceSummary = nodeSummary.getHotResourceSummaryList().get(0);
        Assert.assertEquals(ResourceUtil.FIELD_DATA_CACHE_EVICTION, resourceSummary.getResource());
        Assert.assertEquals(0.01, 6.0, resourceSummary.getValue());

        mockFlowUnits(0, 1.0);
        fieldDataCacheRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(14)));
        flowUnit = fieldDataCacheRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        // TimeWindow 4 of size 300sec
        mockFlowUnits(0, 7.0);
        fieldDataCacheRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(17)));
        flowUnit = fieldDataCacheRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
    }
}
