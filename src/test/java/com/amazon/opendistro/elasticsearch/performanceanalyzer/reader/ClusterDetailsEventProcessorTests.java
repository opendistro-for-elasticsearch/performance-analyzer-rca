/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides.Overrides;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesApplier;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

public class ClusterDetailsEventProcessorTests {

  @Mock
  ConfigOverridesApplier mockOverridesApplier;

  @Captor
  ArgumentCaptor<String> overridesCaptor;

  private ClusterDetailsEventProcessor testClusterDetailsEventProcessor;
  private ConfigOverrides testOverrides;
  private String nodeId1 = "s7gDCVnCSiuBgHoYLji1gw";
  private String address1 = "10.212.49.140";

  private String nodeId2 = "Zn1QcSUGT--DciD1Em5wRg";
  private String address2 = "10.212.52.241";

  private String disabledDecider = "disabled decider";

  @Before
  public void setup() {
    initMocks(this);

    testOverrides = new ConfigOverrides();
    ConfigOverrides.Overrides disabled = new Overrides();
    disabled.setDeciders(Collections.singletonList(disabledDecider));
    testOverrides.setDisable(disabled);

    testClusterDetailsEventProcessor = new ClusterDetailsEventProcessor(mockOverridesApplier);
  }

  @Test
  public void testProcessEvent() throws Exception {

    boolean isMasterNode1 = true;

    boolean isMasterNode2 = false;

    ClusterDetailsEventProcessor clusterDetailsEventProcessor;
    try {
      ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
      clusterDetailsEventProcessorTestHelper.addNodeDetails(nodeId1, address1, isMasterNode1);
      clusterDetailsEventProcessorTestHelper.addNodeDetails(nodeId2, address2, isMasterNode2);
      clusterDetailsEventProcessor = clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
    } catch (Exception e) {
      Assert.assertTrue("got exception when generating cluster details event", false);
      return;
    }

    List<NodeDetails> nodes = clusterDetailsEventProcessor.getNodesDetails();

    assertEquals(nodeId1, nodes.get(0).getId());
    assertEquals(address1, nodes.get(0).getHostAddress());
    assertEquals(isMasterNode1, nodes.get(0).getIsMasterNode());

    assertEquals(nodeId2, nodes.get(1).getId());
    assertEquals(address2, nodes.get(1).getHostAddress());
    assertEquals(isMasterNode2, nodes.get(1).getIsMasterNode());
  }

  @Test
  public void testApplyOverrides()
      throws Exception {
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails(nodeId1, address1, true);
    clusterDetailsEventProcessorTestHelper.addNodeDetails(nodeId2, address2, false);

    Event testEvent = clusterDetailsEventProcessorTestHelper
        .generateTestEventWithOverrides(testOverrides);

    testClusterDetailsEventProcessor.processEvent(testEvent);

    verify(mockOverridesApplier).applyOverride(overridesCaptor.capture(), anyString());

    ObjectMapper mapper = new ObjectMapper();
    ConfigOverrides capturedOverride = mapper.readValue(overridesCaptor.getValue(),
        ConfigOverrides.class);

    assertNotNull(capturedOverride.getDisable());
    assertNotNull(capturedOverride.getDisable().getDeciders());
    assertEquals(testOverrides.getDisable().getDeciders().size(),
        capturedOverride.getDisable().getDeciders().size());
    assertEquals(disabledDecider, capturedOverride.getDisable().getDeciders().get(0));
  }
}
