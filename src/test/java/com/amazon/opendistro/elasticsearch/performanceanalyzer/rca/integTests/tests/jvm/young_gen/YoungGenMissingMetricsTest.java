/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvm.young_gen;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCInfoDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.Constants;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Time;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Type;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.TestApi;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners.RcaItNotEncryptedRunner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvm.young_gen.validator.YoungGenNonBreachingValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvmsizing.JvmSizingITConstants;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Negative test: Tests that the action is NOT emitted if the required metrics are missing.
 */

@Category(RcaItMarker.class)
@RunWith(RcaItNotEncryptedRunner.class)
@AClusterType(ClusterType.MULTI_NODE_CO_LOCATED_MASTER)
@ARcaGraph(ElasticSearchAnalysisGraph.class)
@ARcaConf(dataNode = YoungGenITConstants.RCA_CONF_PATH, electedMaster =
    YoungGenITConstants.RCA_MASTER_CONF_PATH)

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
                    dimensionValues = {GCType.Constants.TOT_FULL_GC_VALUE},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        )
    }
)
@AMetric(
    name = GC_Type.class,
    dimensionNames = {GCInfoDimension.Constants.MEMORY_POOL_VALUE,
        GCInfoDimension.Constants.COLLECTOR_NAME_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE,
                        JvmSizingITConstants.CMS_COLLECTOR},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE,
                        JvmSizingITConstants.CMS_COLLECTOR},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        )
    }
)
public class YoungGenMissingMetricsTest {

  public static final int SLEEP_DURATION_IN_S = 190;

  private TestApi testApi;

  @Test
  @AExpect(
      what = Type.REST_API,
      on = HostTag.ELECTED_MASTER,
      validator = YoungGenNonBreachingValidator.class,
      forRca = PersistedAction.class,
      timeoutSeconds = 240
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
      pattern = "HighHeapUsageYoungGenRca:operate()",
      reason = "YoungGen metrics is expected to be missing."
  )
  @AErrorPatternIgnored(
      pattern = "PersistableSlidingWindow:<init>()",
      reason = "Persistence base path can be null for integration test."
  )
  @AErrorPatternIgnored(
      pattern = "OldGenRca:getMaxHeapSizeOrDefault()",
      reason = "YoungGen metrics is expected to be missing.")
  @AErrorPatternIgnored(
      pattern = "BucketizedSlidingWindow:next()",
      reason = "Since the persistence path can be null for integration test, calls to next() is "
          + "expected to fail")
  public void testDataNodeMissingGcMetrics() {
    // We know that it takes at most 180 seconds(in the case of a multinode cluster) to fire the
    // action based on the decider and rca thresholds set for integration tests.
    // In order to prove that no action has been taken, we sleep for 190 seconds in the test
    // while the rest of the framework goes on emitting metrics, ticking the scheduler etc. On
    // wakeup, we check if the actions table contains the relevant row.

    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(SLEEP_DURATION_IN_S));
    } catch (InterruptedException e) {
      throw new RuntimeException("Sleep was interrupted. Underlying exception: ", e);
    }
  }

  @Test
  @AExpect(
      what = Type.REST_API,
      on = HostTag.ELECTED_MASTER,
      validator = YoungGenNonBreachingValidator.class,
      forRca = PersistedAction.class,
      timeoutSeconds = 240
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
      pattern = "HighHeapUsageYoungGenRca:operate()",
      reason = "YoungGen metrics is expected to be missing."
  )
  @AErrorPatternIgnored(
      pattern = "PersistableSlidingWindow:<init>()",
      reason = "Persistence base path can be null for integration test."
  )
  @AErrorPatternIgnored(
      pattern = "OldGenRca:getMaxHeapSizeOrDefault()",
      reason = "YoungGen metrics is expected to be missing.")
  @AErrorPatternIgnored(
      pattern = "BucketizedSlidingWindow:next()",
      reason = "Since the persistence path can be null for integration test, calls to next() is "
          + "expected to fail"
  )
  @AErrorPatternIgnored(
      pattern = "NetServer:subscribe()",
      reason = "Expected for a scheduler restart"
  )
  @AErrorPatternIgnored(
      pattern = "PerformanceAnalyzerApp:readRcaConfig()",
      reason = "Expected for a scheduler restart"
  )
  public void testMissingHeapMetrics() throws Exception {
    // Same reasoning as the test case above.
    testApi.updateMetrics(MissingHeapMetricsAnnotationHolder.class);
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(SLEEP_DURATION_IN_S));
    } catch (InterruptedException e) {
      throw new RuntimeException("Sleep was interrupted. Underlying exception: ", e);
    }
  }

  public void setTestApi(TestApi testApi) {
    this.testApi = testApi;
  }

  @AMetric(name = GC_Collection_Time.class,
      dimensionNames = {Constants.TYPE_VALUE},
      tables = {
          @ATable(hostTag = HostTag.DATA_0,
              tuple = {
                  @ATuple(dimensionValues = {GCType.Constants.TOT_FULL_GC_VALUE},
                      sum = 5000, avg = 5000, min = 5000, max = 5000),
                  @ATuple(dimensionValues = {GCType.Constants.TOT_YOUNG_GC_VALUE},
                      sum = 500, avg = 500, min = 500, max = 500)
              }
          ),
          @ATable(hostTag = HostTag.ELECTED_MASTER,
              tuple = {
                  @ATuple(dimensionValues = {GCType.Constants.TOT_FULL_GC_VALUE},
                      sum = 5000, avg = 5000, min = 5000, max = 5000),
                  @ATuple(dimensionValues = {GCType.Constants.TOT_YOUNG_GC_VALUE},
                      sum = 500, avg = 500, min = 500, max = 500)
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
                      dimensionValues = {GCType.Constants.TOT_FULL_GC_VALUE},
                      sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                  )
              }
          )
      }
  )
  @AMetric(
      name = GC_Type.class,
      dimensionNames = {GCInfoDimension.Constants.MEMORY_POOL_VALUE,
          GCInfoDimension.Constants.COLLECTOR_NAME_VALUE},
      tables = {
          @ATable(
              hostTag = HostTag.DATA_0,
              tuple = {
                  @ATuple(
                      dimensionValues = {GCType.Constants.OLD_GEN_VALUE,
                          JvmSizingITConstants.CMS_COLLECTOR},
                      sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                  )
              }
          ),
          @ATable(
              hostTag = HostTag.ELECTED_MASTER,
              tuple = {
                  @ATuple(
                      dimensionValues = {GCType.Constants.OLD_GEN_VALUE,
                          JvmSizingITConstants.CMS_COLLECTOR},
                      sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                  )
              }
          )
      }
  )
  public static class MissingHeapMetricsAnnotationHolder {
  }
}
