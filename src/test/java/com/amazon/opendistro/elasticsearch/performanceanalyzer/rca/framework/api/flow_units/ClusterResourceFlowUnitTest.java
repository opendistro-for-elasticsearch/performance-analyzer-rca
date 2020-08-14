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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterResourceFlowUnitTest {
  protected RcaConf rcaConf;

  @Before
  public void setup() {
    this.rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
  }

  protected ClusterResourceFlowUnit newEmptyFlowUnit() {
    return new ClusterResourceFlowUnit(System.currentTimeMillis());
  }

  protected ClusterResourceFlowUnit newFlowUnit(ResourceContext context,
      HotClusterSummary summary, RcaConf rcaConf, boolean persistSummary) {
    return new ClusterResourceFlowUnit(System.currentTimeMillis(), context, summary, rcaConf,
        persistSummary);
  }

  protected BucketCalculator expectedBucketCalculator() {
    return null;
  }

  @Test
  public void testEmptyClusterResourceFlowUnit() {
    ClusterResourceFlowUnit flowUnit = newEmptyFlowUnit();
    Assert.assertTrue(flowUnit.isEmpty());
  }

  @Test
  public void testNullSummary() {
    ClusterResourceFlowUnit flowUnit = newFlowUnit(new ResourceContext(State.UNHEALTHY),
        null, rcaConf, true);
    // Make sure this doesn't throw
    flowUnit.computeUsageBuckets();
  }

  @Test
  public void testNullConf() {
    ClusterResourceFlowUnit flowUnit = newFlowUnit(null, new HotClusterSummary(3, 0), null, false);
    flowUnit.computeUsageBuckets();
    Assert.assertEquals(UsageBucket.UNKNOWN,
        flowUnit.getUsageBucket(new NodeKey(new Id("A"), new Ip("127.0.0.1")), ResourceEnum.OLD_GEN));
    Assert.assertNull(flowUnit.initBucketCalculator());
    flowUnit = new ClusterResourceFlowUnit(System.currentTimeMillis(),
        null, null, null, false);
    flowUnit.computeUsageBuckets();
    Assert.assertEquals(UsageBucket.UNKNOWN,
        flowUnit.getUsageBucket(new NodeKey(new Id("A"), new Ip("127.0.0.1")), ResourceEnum.OLD_GEN));
  }

  @Test
  public void testInitBucketCalculator() {
    Assert.assertEquals(expectedBucketCalculator(), newFlowUnit(null, null, rcaConf, false).bucketCalculator);
  }
}
