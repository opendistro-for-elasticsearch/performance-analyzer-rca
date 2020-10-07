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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.MINOR_GC_PAUSE_TIME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.YOUNG_GEN_PROMOTION_RATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.JvmGenerationAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.DeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.JvmGenerationTuningPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JvmGenerationTuningPolicyTest {
  private JvmGenerationTuningPolicy policy;
  @Mock
  private HighHeapUsageClusterRca rca;
  @Mock
  private JvmGenerationTuningPolicyConfig policyConfig;
  @Mock
  private RcaConf rcaConf;
  @Mock
  private AppContext appContext;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    MockitoAnnotations.initMocks(this);
    policy = new JvmGenerationTuningPolicy(rca);
    policy.setRcaConf(rcaConf);
    policy.setAppContext(appContext);
    DeciderConfig deciderConfig = mock(DeciderConfig.class);
    when(rcaConf.getDeciderConfig()).thenReturn(deciderConfig);
    when(deciderConfig.getJvmGenerationTuningPolicyConfig()).thenReturn(policyConfig);
    when(policyConfig.getSlidingWindowSizeInSeconds()).thenReturn(3600);
    ResourceFlowUnit<HotClusterSummary> flowUnit = (ResourceFlowUnit<HotClusterSummary>) mock(ResourceFlowUnit.class);
    when(rca.getFlowUnits()).thenReturn(Collections.singletonList(flowUnit));
  }

  /**
   * After a call to this function, rca will return a FlowUnit containing n issues with resource
   *
   * <p>This is useful because the policy suggests actions based on resources and the count of issues
   * observed for those resources.
   *
   * @param n the number of young gen issues
   */
  private void mockRcaIssues(int n, Resource resource) {
    ResourceFlowUnit<HotClusterSummary> flowUnit = new ResourceFlowUnit<>(1);
    HotNodeSummary nodeSummary = new HotNodeSummary(new Id("A"), new Ip("127.0.0.1"));
    for (int i = 0; i < n; i++) {
      nodeSummary.appendNestedSummary(new HotResourceSummary(resource, 1, 1, 1));
    }
    HotClusterSummary hotClusterSummary = new HotClusterSummary(3, 1);
    hotClusterSummary.appendNestedSummary(nodeSummary);
    flowUnit.setSummary(hotClusterSummary);
    when(rca.getFlowUnits()).thenReturn(Collections.singletonList(flowUnit));
  }

  /**
   * Causes the policy to view the old:young gen size ratio as desiredRatio
   * @param desiredRatio the old:young gen size ratio; 3 means the old gen is 3X larger than young
   */
  private void mockCurrentRatio(double desiredRatio) {
    NodeConfigCache cache = mock(NodeConfigCache.class);
    when(appContext.getNodeConfigCache()).thenReturn(cache);
    when(appContext.getDataNodeInstances()).thenReturn(Collections.singletonList(
        new InstanceDetails(NodeRole.DATA, new Id("A"), new Ip("127.0.0.1"), false)));
    when(cache.get(any(), eq(ResourceUtil.OLD_GEN_MAX_SIZE))).thenReturn(desiredRatio);
    when(cache.get(any(), eq(ResourceUtil.YOUNG_GEN_MAX_SIZE))).thenReturn(1d);
  }

  /**
   * Clears any issues recorded by the policy so far
   */
  private void clearPolicy() {
    policy.tooLargeIssues.clear();
    policy.tooSmallIssues.clear();
  }

  /**
   * When one of rcaConf, appContext is null, policy evaluation should return no actions
   */
  @Test
  public void testEvaluate_withoutContext() {
    policy.setRcaConf(null);
    Assert.assertTrue(policy.evaluate().isEmpty());
    policy.setAppContext(null);
    Assert.assertTrue(policy.evaluate().isEmpty());
    policy.setRcaConf(rcaConf);
    Assert.assertTrue(policy.evaluate().isEmpty());
  }

  /**
   * Tests that the evaluate() method returns the correct actions in various scenarios
   */
  @Test
  public void testEvaluate() {
    // The policy should not emit actions when it is disabled
    when(policyConfig.isEnabled()).thenReturn(false);
    Assert.assertTrue(policy.evaluate().isEmpty());
    when(policyConfig.isEnabled()).thenReturn(true);
    // Neither generation has had enough issues
    mockRcaIssues(1, MINOR_GC_PAUSE_TIME);
    // The policy should not suggest any actions when there are no issues
    when(policyConfig.shouldDecreaseYoungGen()).thenReturn(false);
    Assert.assertTrue(policy.evaluate().isEmpty());
    // Make the young generation seem oversized
    mockRcaIssues(10, MINOR_GC_PAUSE_TIME);
    // The policy should not suggest decreasing young gen when the option is disabled
    when(policyConfig.shouldDecreaseYoungGen()).thenReturn(false);
    Assert.assertTrue(policy.evaluate().isEmpty());
    // The policy should suggest decreasing young gen when it is oversized and the option is enabled
    when(policyConfig.shouldDecreaseYoungGen()).thenReturn(true);
    mockCurrentRatio(4);
    List<Action> actions = policy.evaluate();
    Assert.assertEquals(1, actions.size());
    Assert.assertTrue(actions.get(0) instanceof JvmGenerationAction);
    Assert.assertEquals(5, ((JvmGenerationAction) actions.get(0)).getTargetRatio());
    // Make the young generation seem undersized
    clearPolicy();
    mockRcaIssues(10, YOUNG_GEN_PROMOTION_RATE);
    // The policy should suggest increasing young gen when it is undersized
    mockCurrentRatio(50);
    actions = policy.evaluate();
    Assert.assertEquals(1, actions.size());
    Assert.assertTrue(actions.get(0) instanceof JvmGenerationAction);
    Assert.assertEquals(3, ((JvmGenerationAction) actions.get(0)).getTargetRatio());
  }
}
