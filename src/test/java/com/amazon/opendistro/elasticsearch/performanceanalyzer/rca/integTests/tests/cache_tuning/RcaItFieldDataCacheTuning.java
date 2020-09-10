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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.RcaItShardRequestCacheTuning.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.RcaItShardRequestCacheTuning.SHARD_ID;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Eviction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Max_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Eviction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Hit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Size;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.Consts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners.RcaItNotEncryptedRunner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.validator.FieldDataCacheActionValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.validator.FieldDataCacheRcaValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.ActionsSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.FieldDataCacheClusterRca;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(RcaItMarker.class)
@RunWith(RcaItNotEncryptedRunner.class)
@AClusterType(ClusterType.MULTI_NODE_CO_LOCATED_MASTER)
@ARcaGraph(ElasticSearchAnalysisGraph.class)
//specify a custom rca.conf to set the collector time periods to 5s to reduce runtime
@ARcaConf(dataNode = RcaItFieldDataCacheTuning.CACHE_TUNING_RESOURCES_DIR + "rca.conf")
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
                sum = 8500.0, avg = 8500.0, min = 8500.0, max = 8500.0),
          }),
      @ATable(
          hostTag = {HostTag.ELECTED_MASTER},
          tuple = {
            @ATuple(
                dimensionValues = {"Index1", "1"},
                sum = 100.0, avg = 100.0, min = 100.0, max = 100.0),
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
                sum = 1.0, avg = 1.0, min = 1.0, max = 1.0),
          }),
      @ATable(
          hostTag = {HostTag.ELECTED_MASTER},
          tuple = {
            @ATuple(
                dimensionValues = {INDEX_NAME, SHARD_ID},
                sum = 0.0, avg = 0.0, min = 0.0, max = 0.0),
          })
    })
@AMetric(
    name = Cache_Request_Size.class,
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
                sum = 100.0, avg = 100.0, min = 100.0, max = 100.0),
          }),
      @ATable(
          hostTag = {HostTag.ELECTED_MASTER},
          tuple = {
            @ATuple(
                dimensionValues = {INDEX_NAME, SHARD_ID},
                sum = 50.0, avg = 50.0, min = 50.0, max = 50.0),
          })
    })
@AMetric(
    name = Cache_Request_Eviction.class,
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
                sum = 0.0, avg = 0.0, min = 0.0, max = 0.0),
          }),
      @ATable(
          hostTag = {HostTag.ELECTED_MASTER},
          tuple = {
            @ATuple(
                dimensionValues = {INDEX_NAME, SHARD_ID},
                sum = 0.0, avg = 0.0, min = 0.0, max = 0.0),
          })
    })
@AMetric(
    name = Cache_Request_Hit.class,
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
                sum = 0.0, avg = 0.0, min = 0.0, max = 0.0),
          }),
      @ATable(
          hostTag = {HostTag.ELECTED_MASTER},
          tuple = {
            @ATuple(
                dimensionValues = {INDEX_NAME, SHARD_ID},
                sum = 0.0, avg = 0.0, min = 0.0, max = 0.0),
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
                sum = 10000.0, avg = 10000.0, min = 10000.0, max = 10000.0),
            @ATuple(
                dimensionValues = {AllMetrics.CacheType.Constants.SHARD_REQUEST_CACHE_NAME},
                sum = 100.0, avg = 100.0, min = 100.0, max = 100.0),
          }),
      @ATable(
          hostTag = {HostTag.ELECTED_MASTER},
          tuple = {
            @ATuple(
                dimensionValues = {AllMetrics.CacheType.Constants.FIELD_DATA_CACHE_NAME},
                sum = 10000.0, avg = 10000.0, min = 10000.0, max = 10000.0),
            @ATuple(
                dimensionValues = {AllMetrics.CacheType.Constants.SHARD_REQUEST_CACHE_NAME},
                sum = 100.0, avg = 100.0, min = 100.0, max = 100.0)
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
                sum = 1000000.0, avg = 1000000.0, min = 1000000.0, max = 1000000.0),
          @ATuple(
                dimensionValues = {AllMetrics.GCType.Constants.HEAP_VALUE},
                sum = 1000000.0, avg = 1000000.0, min = 1000000.0, max = 1000000.0)
          }),
      @ATable(
          hostTag = {HostTag.ELECTED_MASTER},
          tuple = {
            @ATuple(
                dimensionValues = {AllMetrics.GCType.Constants.HEAP_VALUE},
                sum = 1000000.0, avg = 1000000.0, min = 1000000.0, max = 1000000.0),
            @ATuple(
                dimensionValues = {AllMetrics.GCType.Constants.HEAP_VALUE},
                sum = 1000000.0, avg = 1000000.0, min = 1000000.0, max = 1000000.0)
          })
    })

public class RcaItFieldDataCacheTuning {
  public static final String CACHE_TUNING_RESOURCES_DIR = Consts.INTEG_TESTS_SRC_DIR + "./tests/cache_tuning/resource/";
  public static final String INDEX_NAME = "MockIndex";
  public static final String SHARD_ID = "1";

  // Test FieldDataCacheClusterRca.
  // This rca should be un-healthy when cache size is higher than threshold with evictions.
  @Test
  @AExpect(
      what = AExpect.Type.REST_API,
      on = HostTag.ELECTED_MASTER,
      validator = FieldDataCacheRcaValidator.class,
      forRca = FieldDataCacheClusterRca.class,
      timeoutSeconds = 700)
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
          pattern = "CacheUtil:getCacheMaxSize()",
          reason = "Node Config Cache is expected to be missing during startup.")
  @AErrorPatternIgnored(
          pattern = "ModifyCacheMaxSizeAction:build()",
          reason = "Heap metrics is expected to be missing during startup.")
  @AErrorPatternIgnored(
          pattern = "SubscribeResponseHandler:onError()",
          reason = "A unit test expressly calls SubscribeResponseHandler#onError, which writes an error log")
  @AErrorPatternIgnored(
          pattern = "SQLParsingUtil:readDataFromSqlResult()",
          reason = "Old gen metrics is expected to be missing in this integ test.")
  @AErrorPatternIgnored(
          pattern = "HighHeapUsageOldGenRca:operate()",
          reason = "Old gen rca is expected to be missing in this integ test.")
  public void testFieldDataCacheRca() {}

  // Test CacheDecider for ModifyCacheAction (field data cache).
  // The cache decider should emit modify cache size action as field data rca is unhealthy.
  @Test
  @AExpect(
      what = AExpect.Type.REST_TABLE_API,
      on = HostTag.ELECTED_MASTER,
      validator = FieldDataCacheActionValidator.class,
      forRca = ActionsSummary.class,
      timeoutSeconds = 700)
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
          pattern = "CacheUtil:getCacheMaxSize()",
          reason = "Node Config Cache is expected to be missing during startup.")
  @AErrorPatternIgnored(
          pattern = "ModifyCacheMaxSizeAction:build()",
          reason = "Heap metrics is expected to be missing during startup.")
  @AErrorPatternIgnored(
          pattern = "SubscribeResponseHandler:onError()",
          reason = "A unit test expressly calls SubscribeResponseHandler#onError, which writes an error log")
  @AErrorPatternIgnored(
          pattern = "SQLParsingUtil:readDataFromSqlResult()",
          reason = "Old gen metrics is expected to be missing in this integ test.")
  @AErrorPatternIgnored(
          pattern = "HighHeapUsageOldGenRca:operate()",
          reason = "Old gen rca is expected to be missing in this integ test.")
  public void testFieldDataCacheAction() {}
}
