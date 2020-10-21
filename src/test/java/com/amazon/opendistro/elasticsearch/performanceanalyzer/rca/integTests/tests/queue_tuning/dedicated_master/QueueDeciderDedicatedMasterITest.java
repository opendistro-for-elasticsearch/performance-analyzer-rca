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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.queue_tuning.dedicated_master;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.queue_tuning.Constants.QUEUE_TUNING_RESOURCES_DIR;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_RejectedReqs;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.queue_tuning.validator.QDeciderNoActionOnUnhealthyValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.queue_tuning.validator.QueueDeciderValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(RcaItNotEncryptedRunner.class)

@Category(RcaItMarker.class)
@AClusterType(ClusterType.MULTI_NODE_DEDICATED_MASTER)
@ARcaGraph(ElasticSearchAnalysisGraph.class)
//specify a custom rca.conf to set the rejection-time-period-in-seconds to 5s to reduce runtime
@ARcaConf(dataNode = QUEUE_TUNING_RESOURCES_DIR + "rca.conf")
@AMetric(name = ThreadPool_RejectedReqs.class,
    dimensionNames = {ThreadPoolDimension.Constants.TYPE_VALUE},
    tables = {
        @ATable(hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(dimensionValues = {ThreadPoolType.Constants.WRITE_NAME},
                    sum = 1.0, avg = 1.0, min = 1.0, max = 1.0),
                @ATuple(dimensionValues = {ThreadPoolType.Constants.SEARCH_NAME},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 0.0)
            }
        )
    }
)

@AMetric(name = ThreadPool_QueueCapacity.class,
    dimensionNames = {ThreadPoolDimension.Constants.TYPE_VALUE},
    tables = {
        @ATable(hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(dimensionValues = {ThreadPoolType.Constants.WRITE_NAME},
                    sum = 500, avg = 500, min = 500, max = 500),
                @ATuple(dimensionValues = {ThreadPoolType.Constants.SEARCH_NAME},
                    sum = 1500, avg = 1500, min = 1500, max = 1500)
            }
        )
    }
)
public class QueueDeciderDedicatedMasterITest {
  // This integ test is built to test Decision Maker framework and queue remediation actions
  // This test injects queue rejection metrics on one of the data node and queries the
  // sqlite table on master to check whether queue remediation actions has been published.
  @Test
  @AExpect(
      what = AExpect.Type.DB_QUERY,
      on = HostTag.ELECTED_MASTER,
      validator = QueueDeciderValidator.class,
      forRca = PersistedAction.class,
      timeoutSeconds = 1000)
  @AErrorPatternIgnored(pattern = "CacheUtil:getCacheMaxSize()",
      reason = "Cache related configs are expected to be missing in this integ test")
  @AErrorPatternIgnored(pattern = "AggregateMetric:gather()",
      reason = "Cache metrics are expected to be missing in this integ test")
  @AErrorPatternIgnored(pattern = "SubscribeResponseHandler:onError()",
      reason = "A unit test expressly calls SubscribeResponseHandler#onError, which writes an error log")
  @AErrorPatternIgnored(pattern = "NodeConfigCacheReaderUtil:readQueueCapacity()",
      reason = "Metrics is expected to be missing")
  @AErrorPatternIgnored(pattern = "ModifyQueueCapacityAction:build()",
      reason = "Metrics is expected to be missing")
  public void testQueueCapacityDecider() {
  }


  @AMetric(name = ThreadPool_RejectedReqs.class,
      dimensionNames = {ThreadPoolDimension.Constants.TYPE_VALUE},
      tables = {
          @ATable(hostTag = HostTag.DATA_0,
              tuple = {
                  @ATuple(dimensionValues = {ThreadPoolType.Constants.WRITE_NAME},
                      sum = 1.0, avg = 1.0, min = 1.0, max = 1.0),
                  @ATuple(dimensionValues = {ThreadPoolType.Constants.SEARCH_NAME},
                      sum = 0.0, avg = 0.0, min = 0.0, max = 0.0)
              }
          )
      }
  )

  @AMetric(name = ThreadPool_QueueCapacity.class,
      dimensionNames = {ThreadPoolDimension.Constants.TYPE_VALUE},
      tables = {
          @ATable(hostTag = HostTag.DATA_0,
              tuple = {
                  @ATuple(dimensionValues = {ThreadPoolType.Constants.WRITE_NAME},
                      sum = 500, avg = 500, min = 500, max = 500),
                  @ATuple(dimensionValues = {ThreadPoolType.Constants.SEARCH_NAME},
                      sum = 1500, avg = 1500, min = 1500, max = 1500)
              }
          )
      }
  )

  @AMetric(name = Heap_Used.class,
      dimensionNames = {AllMetrics.HeapDimension.Constants.TYPE_VALUE},
      tables = {
          @ATable(hostTag = {HostTag.DATA_0, HostTag.DATA_1},
              tuple = {
                  @ATuple(dimensionValues = AllMetrics.GCType.Constants.OLD_GEN_VALUE,
                      sum = 65695944, avg = 65695944, min = 65695944, max = 65695944),
              }
          )
      }
  )
  @AMetric(name = GC_Collection_Event.class,
      dimensionNames = {AllMetrics.HeapDimension.Constants.TYPE_VALUE},
      tables = {
          @ATable(hostTag = {HostTag.DATA_0, HostTag.DATA_1},
              tuple = {
                  @ATuple(dimensionValues = AllMetrics.GCType.Constants.TOT_FULL_GC_VALUE,
                      sum = 2, avg = 2, min = 2, max = 2),
              }
          )
      }
  )
  @AMetric(name = Heap_Max.class,
      dimensionNames = {AllMetrics.HeapDimension.Constants.TYPE_VALUE},
      tables = {
          @ATable(hostTag = {HostTag.DATA_0, HostTag.DATA_1},
              tuple = {
                  @ATuple(dimensionValues = AllMetrics.GCType.Constants.OLD_GEN_VALUE,
                      sum = 65695945, avg = 65695945, min = 65695945, max = 65695945),
              }
          )
      }
  )

  @Test
  @AExpect(
      what = AExpect.Type.DB_QUERY,
      on = HostTag.ELECTED_MASTER,
      validator = QDeciderNoActionOnUnhealthyValidator.class,
      forRca = PersistedAction.class,
      timeoutSeconds = 1000)
  @AErrorPatternIgnored(pattern = "CacheUtil:getCacheMaxSize()",
      reason = "Cache related configs are expected to be missing in this integ test")
  @AErrorPatternIgnored(pattern = "AggregateMetric:gather()",
      reason = "Cache metrics are expected to be missing in this integ test")
  @AErrorPatternIgnored(pattern = "SubscribeResponseHandler:onError()",
      reason = "A unit test expressly calls SubscribeResponseHandler#onError, which writes an error log")
  @AErrorPatternIgnored(pattern = "NodeConfigCollector:collectAndPublishMetric()",
      reason = "Metrics is expected to be missing")
  @AErrorPatternIgnored(pattern = "NodeConfigCollector:readQueueCapacity()",
      reason = "Metrics is expected to be missing")
  @AErrorPatternIgnored(pattern = "NodeConfigCacheReaderUtil:readQueueCapacity()",
      reason = "Metrics is expected to be missing")
  @AErrorPatternIgnored(pattern = "ModifyQueueCapacityAction:build()",
      reason = "Metrics is expected to be missing")
  public void testNoCapacityIncreaseOnUnHealthy() {
  }
}
