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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.CPU;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.DISK;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.HEAP;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.NETWORK;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.RAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.QueueActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableSet;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class ModifyQueueCapacityActionTest {

  private AppContext testAppContext;
  private NodeConfigCache dummyCache;
  private RcaConf rcaConf;

  public ModifyQueueCapacityActionTest() {
    testAppContext = new AppContext();
    dummyCache = testAppContext.getNodeConfigCache();
    String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString();
    rcaConf = new RcaConf(rcaConfPath);
  }

  @Test
  public void testIncreaseCapacity() {
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.WRITE_QUEUE_CAPACITY, 500);
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.WRITE_THREADPOOL, testAppContext, rcaConf);
    ModifyQueueCapacityAction modifyQueueCapacityAction = builder.increase(true).build();
    Assert.assertNotNull(modifyQueueCapacityAction);
    assertTrue(modifyQueueCapacityAction.getDesiredCapacity() > modifyQueueCapacityAction.getCurrentCapacity());
    assertTrue(modifyQueueCapacityAction.isActionable());
    assertEquals(QueueActionConfig.DEFAULT_COOL_OFF_PERIOD_IN_SECONDS * 1_000,
            modifyQueueCapacityAction.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.WRITE_THREADPOOL, modifyQueueCapacityAction.getThreadPool());
    assertEquals(1, modifyQueueCapacityAction.impactedNodes().size());

    Map<Dimension, Impact> impact = modifyQueueCapacityAction.impact().get(node1).getImpact();
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(HEAP));
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(CPU));
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  @Test
  public void testDecreaseCapacity() {
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 1500);
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL, testAppContext, rcaConf);
    ModifyQueueCapacityAction modifyQueueCapacityAction = builder.increase(false).build();
    Assert.assertNotNull(modifyQueueCapacityAction);
    assertTrue(modifyQueueCapacityAction.getDesiredCapacity() < modifyQueueCapacityAction.getCurrentCapacity());
    assertTrue(modifyQueueCapacityAction.isActionable());
    assertEquals(QueueActionConfig.DEFAULT_COOL_OFF_PERIOD_IN_SECONDS * 1_000,
            modifyQueueCapacityAction.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.SEARCH_THREADPOOL, modifyQueueCapacityAction.getThreadPool());
    assertEquals(1, modifyQueueCapacityAction.impactedNodes().size());

    Map<Dimension, Impact> impact = modifyQueueCapacityAction.impact().get(node1).getImpact();
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(HEAP));
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(CPU));
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  @Test
  public void testBounds() throws Exception {
    final String configStr =
      "{"
          + "\"action-config-settings\": { "
              + "\"queue-settings\": { "
                  + "\"search\": { "
                      + "\"upper-bound\": 500, "
                      + "\"lower-bound\": 100 "
                  + "}, "
                  + "\"write\": { "
                      + "\"upper-bound\": 50, "
                      + "\"lower-bound\": 10 "
                  + "} "
              + "} "
          + "} "
      + "}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);

    // Test Upper Bound
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 500);
    ModifyQueueCapacityAction searchQueueAction = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL,
        testAppContext, conf).increase(true).build();
    assertEquals(500, searchQueueAction.getDesiredCapacity());
    assertFalse(searchQueueAction.isActionable());

    dummyCache.put(node1, ResourceUtil.WRITE_QUEUE_CAPACITY, 50);
    ModifyQueueCapacityAction writeQueueAction = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.WRITE_THREADPOOL,
        testAppContext, conf).increase(true).build();
    assertEquals(50, writeQueueAction.getDesiredCapacity());
    assertFalse(writeQueueAction.isActionable());

    // Test Lower Bound
    node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 100);
    searchQueueAction = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL,
        testAppContext, conf).increase(false).build();
    assertEquals(100, searchQueueAction.getDesiredCapacity());
    assertFalse(searchQueueAction.isActionable());

    dummyCache.put(node1, ResourceUtil.WRITE_QUEUE_CAPACITY, 10);
    writeQueueAction = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.WRITE_THREADPOOL,
        testAppContext, conf).increase(false).build();
    assertEquals(10, writeQueueAction.getDesiredCapacity());
    assertFalse(writeQueueAction.isActionable());
  }

  @Test
  public void testMinMaxOverrides() throws Exception {
    final String configStr =
      "{"
          + "\"action-config-settings\": { "
              + "\"queue-settings\": { "
                  + "\"search\": { "
                      + "\"upper-bound\": 500, "
                      + "\"lower-bound\": 100 "
                  + "}, "
                  + "\"write\": { "
                      + "\"upper-bound\": 50, "
                      + "\"lower-bound\": 10 "
                  + "} "
              + "} "
          + "} "
      + "}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);

    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 200);
    dummyCache.put(node1, ResourceUtil.WRITE_QUEUE_CAPACITY, 20);

    // Test Max Override
    ModifyQueueCapacityAction searchQueueAction = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL,
        testAppContext, conf).setDesiredCapacityToMax().build();
    assertEquals(500, searchQueueAction.getDesiredCapacity());
    assertTrue(searchQueueAction.isActionable());

    ModifyQueueCapacityAction writeQueueAction = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.WRITE_THREADPOOL,
        testAppContext, conf).setDesiredCapacityToMax().build();
    assertEquals(50, writeQueueAction.getDesiredCapacity());
    assertTrue(writeQueueAction.isActionable());

    // Test Min Override
    searchQueueAction = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL,
        testAppContext, conf).setDesiredCapacityToMin().build();
    assertEquals(100, searchQueueAction.getDesiredCapacity());
    assertTrue(searchQueueAction.isActionable());

    writeQueueAction = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.WRITE_THREADPOOL,
        testAppContext, conf).setDesiredCapacityToMin().build();
    assertEquals(10, writeQueueAction.getDesiredCapacity());
    assertTrue(writeQueueAction.isActionable());
  }

  @Test
  public void testMutedAction() {
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 2000);
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL, testAppContext, rcaConf);
    ModifyQueueCapacityAction modifyQueueCapacityAction = builder.increase(true).build();

    testAppContext.updateMutedActions(ImmutableSet.of(modifyQueueCapacityAction.name()));

    assertFalse(modifyQueueCapacityAction.isActionable());
  }

  private void assertNoImpact(NodeKey node, ModifyQueueCapacityAction modifyQueueCapacityAction) {
    Map<Dimension, Impact> impact = modifyQueueCapacityAction.impact().get(node).getImpact();
    assertEquals(Impact.NO_IMPACT, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }
}
