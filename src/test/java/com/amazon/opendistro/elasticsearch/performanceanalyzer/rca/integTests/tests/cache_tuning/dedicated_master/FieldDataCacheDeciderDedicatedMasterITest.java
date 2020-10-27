/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.dedicated_master;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.Constants.CACHE_TUNING_RESOURCES_DIR;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.Constants.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.Constants.SHARD_ID;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Eviction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Max_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.RcaItMarker;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AErrorPatternIgnored;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AExpect;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ARcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ARcaGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATuple;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners.RcaItNotEncryptedRunner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.validator.FieldDataCacheDeciderValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(RcaItMarker.class)
@RunWith(RcaItNotEncryptedRunner.class)
@AClusterType(ClusterType.MULTI_NODE_CO_LOCATED_MASTER)
@ARcaGraph(ElasticSearchAnalysisGraph.class)
//specify a custom rca.conf to set the collector time periods to 5s to reduce runtime
@ARcaConf(dataNode = CACHE_TUNING_RESOURCES_DIR + "rca.conf")
@AMetric(
        name = Cache_FieldData_Size.class,
        dimensionNames = {
                AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE,
                AllMetrics.CommonDimension.Constants.SHARDID_VALUE
        },
        tables = {
                @ATable(
                        hostTag = HostTag.DATA_0,
                        tuple = {
                                @ATuple(
                                        dimensionValues = {INDEX_NAME, SHARD_ID},
                                        sum = 8500.0, avg = 8500.0, min = 8500.0, max = 8500.0)
                        })
        })
@AMetric(
        name = Cache_FieldData_Eviction.class,
        dimensionNames = {
                AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE,
                AllMetrics.CommonDimension.Constants.SHARDID_VALUE
        },
        tables = {
                @ATable(
                        hostTag = HostTag.DATA_0,
                        tuple = {
                                @ATuple(
                                        dimensionValues = {INDEX_NAME, SHARD_ID},
                                        sum = 1.0, avg = 1.0, min = 1.0, max = 1.0)
                        })
        })
@AMetric(
        name = Cache_Max_Size.class,
        dimensionNames = {AllMetrics.CacheConfigDimension.Constants.TYPE_VALUE},
        tables = {
                @ATable(
                        hostTag = HostTag.DATA_0,
                        tuple = {
                                @ATuple(
                                        dimensionValues = {AllMetrics.CacheType.Constants.FIELD_DATA_CACHE_NAME},
                                        sum = 10000.0, avg = 10000.0, min = 10000.0, max = 10000.0)
                        }),
                @ATable(
                        hostTag = HostTag.ELECTED_MASTER,
                        tuple = {
                                @ATuple(
                                        dimensionValues = {AllMetrics.CacheType.Constants.FIELD_DATA_CACHE_NAME},
                                        sum = 10000.0, avg = 10000.0, min = 10000.0, max = 10000.0)
                        })
        })
@AMetric(
        name = Heap_Max.class,
        dimensionNames = {AllMetrics.HeapDimension.Constants.TYPE_VALUE},
        tables = {
                @ATable(
                        hostTag = HostTag.DATA_0,
                        tuple = {
                                @ATuple(
                                        dimensionValues = {AllMetrics.GCType.Constants.HEAP_VALUE},
                                        sum = 1000000.0, avg = 1000000.0, min = 1000000.0, max = 1000000.0)
                        }),
                @ATable(
                        hostTag = {HostTag.ELECTED_MASTER},
                        tuple = {
                                @ATuple(
                                        dimensionValues = {AllMetrics.GCType.Constants.HEAP_VALUE},
                                        sum = 1000000.0, avg = 1000000.0, min = 1000000.0, max = 1000000.0)
                        })
        })

public class FieldDataCacheDeciderDedicatedMasterITest {
    // Test CacheDecider for ModifyCacheAction (field data cache).
    // The cache decider should emit modify cache size action as field data rca is unhealthy.
    @Test
    @AExpect(
            what = AExpect.Type.DB_QUERY,
            on = HostTag.ELECTED_MASTER,
            validator = FieldDataCacheDeciderValidator.class,
            forRca = PersistedAction.class,
            timeoutSeconds = 1000)
    @AErrorPatternIgnored(
            pattern = "AggregateMetric:gather()",
            reason = "CPU metrics are expected to be missing in this integ test")
    @AErrorPatternIgnored(
            pattern = "Metric:gather()",
            reason = "Metrics are expected to be missing in this integ test")
    @AErrorPatternIgnored(
            pattern = "NodeConfigCacheReaderUtil",
            reason = "Node Config Cache are expected to be missing in this integ test.")
    @AErrorPatternIgnored(
            pattern = "SubscribeResponseHandler:onError()",
            reason = "A unit test expressly calls SubscribeResponseHandler#onError, which writes an error log")
    @AErrorPatternIgnored(
            pattern = "SQLParsingUtil:readDataFromSqlResult()",
            reason = "Old gen metrics is expected to be missing in this integ test.")
    @AErrorPatternIgnored(
            pattern = "HighHeapUsageOldGenRca:operate()",
            reason = "Old gen rca is expected to be missing in this integ test.")
    @AErrorPatternIgnored(
            pattern = "ModifyCacheMaxSizeAction:build()",
            reason = "Node config cache is expected to be missing during shutdown")
    @AErrorPatternIgnored(
            pattern = "NodeConfigCollector:collectAndPublishMetric()",
            reason = "Shard request cache metrics is expected to be missing")
    @AErrorPatternIgnored(
            pattern = "CacheUtil:getCacheMaxSize()",
            reason = "Shard request cache metrics is expected to be missing.")
    @AErrorPatternIgnored(
            pattern = "OldGenRca:getMaxOldGenSizeOrDefault()",
            reason = "Old gen metrics is expected to be missing in this integ test.")
    public void testFieldDataCacheAction() {}
}
