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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class LargeHeapClusterRcaTest {
  @Mock
  private OldGenContendedRca mockOldGenContendedRca;

  @Mock
  private AppContext mockAppContext;

  private LargeHeapClusterRca testRca;
  private final List<InstanceDetails> healthyInstances = Collections.singletonList(
      getInstanceDetail("node1", "1.2.3.4"));
  private final List<InstanceDetails> unhealthyInstances = Collections.singletonList(
      getInstanceDetail("node2", "1.2.3.5"));

  @Before
  public void setup() throws Exception {
    initMocks(this);
    setupAppContext();
    this.testRca = new LargeHeapClusterRca(mockOldGenContendedRca);
    testRca.setAppContext(mockAppContext);
  }

  @Test
  public void testEmptyFlowUnits() {
    when(mockOldGenContendedRca.getFlowUnits()).thenReturn(Collections.emptyList());
    ResourceFlowUnit<HotClusterSummary> flowUnit = testRca.operate();

    assertTrue(flowUnit.isEmpty());
  }

  @Test
  public void testSomeUnhealthyNodes() {
    ResourceFlowUnit<HotNodeSummary> oldGenFlowUnit = new ResourceFlowUnit<>(System.currentTimeMillis(),
        new ResourceContext(State.CONTENDED),
        new HotNodeSummary(unhealthyInstances.get(0).getInstanceId(),
            unhealthyInstances.get(0).getInstanceIp()));
    when(mockOldGenContendedRca.getFlowUnits()).thenReturn(Collections.singletonList(oldGenFlowUnit));

    ResourceFlowUnit<HotClusterSummary> flowUnit = testRca.operate();
    assertEquals(1, flowUnit.getSummary().getNumOfUnhealthyNodes());

    HotClusterSummary summary = flowUnit.getSummary();
    assertEquals(1, summary.getHotNodeSummaryList().size());
    assertEquals(unhealthyInstances.get(0).getInstanceId(),
        summary.getHotNodeSummaryList().get(0).getNodeID());
    assertEquals(unhealthyInstances.get(0).getInstanceIp(),
        summary.getHotNodeSummaryList().get(0).getHostAddress());
  }

  private InstanceDetails getInstanceDetail(final String nodeId, final String hostAddress) {
    return new InstanceDetails(new Id(nodeId), new Ip(hostAddress), 0);
  }

  private void setupAppContext() {
    List<InstanceDetails> allInstances = new ArrayList<>(healthyInstances);
    allInstances.addAll(unhealthyInstances);
    when(mockAppContext.getAllClusterInstances()).thenReturn(allInstances);
  }
}