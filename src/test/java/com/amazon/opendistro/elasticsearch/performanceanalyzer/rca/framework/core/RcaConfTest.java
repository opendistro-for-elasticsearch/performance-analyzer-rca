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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.CacheConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.DeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageOldGenRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageOldGenRcaConfig.RCA_CONF_KEY_CONSTANTS;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageYoungGenRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotNodeClusterRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RcaConfTest {

  private RcaConf rcaConf;

  @Before
  public void init() {
    String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_elected_master.conf").toString();
    rcaConf = new RcaConf(rcaConfPath);
  }

  @Test
  public void testReadRcaConfig() {
    Integer topK = rcaConf.readRcaConfig(HighHeapUsageOldGenRcaConfig.CONFIG_NAME, RCA_CONF_KEY_CONSTANTS.TOP_K, Integer.class);
    Assert.assertNotNull(topK);
    Assert.assertEquals(HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K, topK.intValue());

    Integer promotionRateThreshold = rcaConf.readRcaConfig(HighHeapUsageYoungGenRcaConfig.CONFIG_NAME,
        HighHeapUsageYoungGenRcaConfig.RCA_CONF_KEY_CONSTANTS.PROMOTION_RATE_THRES, Integer.class);
    Assert.assertNotNull(promotionRateThreshold);
    Assert.assertEquals(HighHeapUsageYoungGenRcaConfig.DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC, promotionRateThreshold.intValue());

    Double unbalancedResourceThreshold = rcaConf.readRcaConfig(HotNodeClusterRcaConfig.CONFIG_NAME,
        HotNodeClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.UNBALANCED_RESOURCE_THRES, Double.class);
    Assert.assertNotNull(unbalancedResourceThreshold);
    Assert.assertEquals(HotNodeClusterRcaConfig.DEFAULT_UNBALANCED_RESOURCE_THRES, unbalancedResourceThreshold, 0.01);

    Integer val = rcaConf.readRcaConfig(HotNodeClusterRcaConfig.CONFIG_NAME,
        HotNodeClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.UNBALANCED_RESOURCE_THRES, Integer.class);
    Assert.assertNull(val);

    val = rcaConf.readRcaConfig(HighHeapUsageOldGenRcaConfig.CONFIG_NAME, "test", Integer.class);
    Assert.assertNull(val);
 
    Integer fieldDataTimePeriod = rcaConf.readRcaConfig(CacheConfig.CONFIG_NAME,
                        CacheConfig.RCA_CONF_KEY_CONSTANTS.FIELD_DATA_COLLECTOR_TIME_PERIOD_IN_SEC, Integer.class);
    Assert.assertNull(val);
    Assert.assertEquals(CacheConfig.DEFAULT_FIELD_DATA_COLLECTOR_TIME_PERIOD_IN_SEC, fieldDataTimePeriod.intValue());

    Integer shardRequestTimePeriod = rcaConf.readRcaConfig(CacheConfig.CONFIG_NAME,
                        CacheConfig.RCA_CONF_KEY_CONSTANTS.SHARD_REQUEST_COLLECTOR_TIME_PERIOD_IN_SEC, Integer.class);
    Assert.assertNull(val);
    Assert.assertEquals(CacheConfig.DEFAULT_SHARD_REQUEST_COLLECTOR_TIME_PERIOD_IN_SEC, shardRequestTimePeriod.intValue());
  }

  @Test
  public void testReadDeciderConfig() {
    DeciderConfig configObj = new DeciderConfig(rcaConf);
    Assert.assertNotNull(configObj.getCachePriorityOrder());
    Assert.assertNotNull(configObj.getWorkloadPriorityOrder());
    Assert.assertNotNull(configObj.getFieldDataCacheUpperBound());
    Assert.assertNotNull(configObj.getShardRequestCacheUpperBound());
    Assert.assertEquals(Arrays.asList("test-fielddata-cache", "test-shard-request-cache", "test-query-cache",
            "test-bitset-filter-cache"), configObj.getCachePriorityOrder());
    Assert.assertEquals(10.4, configObj.getFieldDataCacheUpperBound(), 0.01);
    Assert.assertEquals(10.05, configObj.getShardRequestCacheUpperBound(), 0.01);
    Assert.assertEquals(Arrays.asList("test-ingest", "test-search"), configObj.getWorkloadPriorityOrder());
  }
}
