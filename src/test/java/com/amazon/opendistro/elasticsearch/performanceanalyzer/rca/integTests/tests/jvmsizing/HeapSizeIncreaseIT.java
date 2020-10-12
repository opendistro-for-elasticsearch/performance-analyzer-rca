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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvmsizing;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.Constants;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.RcaItMarker;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AErrorPatternIgnored;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AExpect;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AExpect.Type;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ARcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ARcaGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATuple;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners.RcaItNotEncryptedRunner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvmsizing.validator.HeapSizeIncreaseValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(RcaItMarker.class)
@RunWith(RcaItNotEncryptedRunner.class)
@AClusterType(ClusterType.MULTI_NODE_CO_LOCATED_MASTER)
@ARcaGraph(ElasticSearchAnalysisGraph.class)
@ARcaConf(dataNode = JvmSizingITConstants.RCA_CONF_PATH + "rca.conf")
@AMetric(
    name = Heap_Max.class,
    dimensionNames = {Constants.TYPE_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 1000000000.0, avg = 1000000000.0, min = 1000000000.0, max = 1000000000.0
                )
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 1000000000.0, avg = 1000000000.0, max = 1000000000.0, min = 1000000000.0
                )
            }
        )
    }
)
@AMetric(
    name = Heap_Used.class,
    dimensionNames = {Constants.TYPE_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 950000000.0, avg = 950000000.0, min = 950000000.0, max = 950000000.0
                )
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 950000000.0, avg = 950000000.0, min = 950000000.0, max = 950000000.0
                )
            }
        )
    }
)
@AMetric(
    name = GC_Collection_Event.class,
    dimensionNames = {Constants.TYPE_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.TOT_FULL_GC_VALUE},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        )
    }
)
public class HeapSizeIncreaseIT {

  @Test
  @AExpect(
      what = Type.DB_QUERY,
      on = HostTag.ELECTED_MASTER,
      validator = HeapSizeIncreaseValidator.class,
      forRca = PersistedAction.class,
      timeoutSeconds = 1000
  )
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
      pattern = "OldGenRca:getFullGcEventsOrDefault()",
      reason = "Expected to be missing for some reason."
  )
  @AErrorPatternIgnored(
      pattern = "HighHeapUsageYoungGenRca:operate()",
      reason = "YoungGen metrics is expected to be missing."
  )
  public void testHeapSizeIncrease() {

  }
}
