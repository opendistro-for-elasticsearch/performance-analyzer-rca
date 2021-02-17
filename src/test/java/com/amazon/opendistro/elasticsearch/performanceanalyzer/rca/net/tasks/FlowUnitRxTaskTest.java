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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;

@Category(GradleTaskForRca.class)
public class FlowUnitRxTaskTest {

  private static final String TEST_GRAPH_NODE = "testGraphNode";
  private static final String TEST_ES_NODE = "testEsNode";

  private FlowUnitRxTask testFlowUnitRxTask;
  private FlowUnitMessage testFlowUnitMessage = FlowUnitMessage.newBuilder()
                                                               .setGraphNode(TEST_GRAPH_NODE)
                                                               .setEsNode(TEST_ES_NODE)
                                                               .build();

  @Mock
  private NodeStateManager mockNodeStateManager;

  @Mock
  private ReceivedFlowUnitStore mockReceivedFlowUnitStore;

  @Before
  public void setUp() throws Exception {
    initMocks(this);
    testFlowUnitRxTask = new FlowUnitRxTask(mockNodeStateManager, mockReceivedFlowUnitStore,
        testFlowUnitMessage);
  }

  @Test
  public void testEnqueueSuccess() {
    when(mockReceivedFlowUnitStore.enqueue(TEST_GRAPH_NODE, testFlowUnitMessage)).thenReturn(true);

    testFlowUnitRxTask.run();

    verify(mockNodeStateManager).updateReceiveTime(eq(new InstanceDetails.Id(TEST_ES_NODE)), eq(TEST_GRAPH_NODE),
        anyLong());
  }
}