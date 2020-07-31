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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaConfTests {

  @Test
  public void testRcaConfRead() throws Exception {
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_master.conf").toString());

    assertEquals("s3://sifi-store/rcas/", rcaConf.getRcaStoreLoc());
    assertEquals("s3://sifi-store/thresholds/", rcaConf.getThresholdStoreLoc());
    assertEquals(60, rcaConf.getNewRcaCheckPeriodicityMins());
    assertEquals(30, rcaConf.getNewThresholdCheckPeriodicityMins());
    assertEquals(Arrays.asList("ip1", "ip2", "ip3"), rcaConf.getPeerIpList());
    Map<String, String> tagMap = newHashMap();
    tagMap.put("locus", "data-node");
    tagMap.put("disk", "ssd");
    tagMap.put("region", "use1");
    tagMap.put("instance-type", "i3.8xl");
    tagMap.put("domain", "rca-test-cluster");
    for (Map.Entry<String, String> tag : rcaConf.getTagMap().entrySet()) {
      String expectedValue = tagMap.get(tag.getKey());
      assertEquals(expectedValue, tag.getValue());
    }
    assertEquals(Collections.EMPTY_LIST, rcaConf.getMutedRcaList());
  }

  @Test
  public void testGetUsageBucketThresholds() {
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_master.conf").toString());
    List<Double> thresholds = rcaConf.getUsageBucketThresholds("young-gen-heap-promotion-rate");
    ResourceUtil.getResourceTypeName(ResourceUtil.YOUNG_GEN_PROMOTION_RATE);
    ResourceUtil.getResourceTypeName(ResourceUtil.OLD_GEN_HEAP_USAGE);
    Assert.assertEquals((Double) 100.0, thresholds.get(0));
    Assert.assertEquals((Double) 250.0, thresholds.get(1));
    Assert.assertEquals((Double) 400.0, thresholds.get(2));
  }
}
