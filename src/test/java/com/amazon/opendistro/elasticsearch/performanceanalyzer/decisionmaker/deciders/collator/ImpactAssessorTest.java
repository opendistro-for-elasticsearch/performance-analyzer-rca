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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class ImpactAssessorTest {

  @Mock
  private Action mockAction;

  @Mock
  private ImpactAssessment mockAssessment;

  private ImpactAssessor testAssessor;

  @Before
  public void setup() {
    initMocks(this);
    when(mockAction.impactedNodes()).thenReturn(Collections.singletonList(new NodeKey(new Id(
        "other node"), new Ip("2.2.3.4"))));
    this.testAssessor = new ImpactAssessor();
  }

  @Test
  public void testIsImpactAlignedNodeMissingFromOverallImpact() {
    NodeKey nodeKey = new NodeKey(new Id("this node"), new Ip("1.2.3.4"));
    Map<NodeKey, ImpactAssessment> testOverallAssessment = new HashMap<>();
    testOverallAssessment
        .put(nodeKey, new ImpactAssessment(nodeKey));

    boolean isAligned = testAssessor.isImpactAligned(mockAction, testOverallAssessment);

    assertFalse(isAligned);
  }

  @Test
  public void testUndoActionImpactOnOverallAssessmentNodeMissing() {
    NodeKey nodeKey = new NodeKey(new Id("this node"), new Ip("1.2.3.4"));
    Map<NodeKey, ImpactAssessment> testOverallAssessment = new HashMap<>();
    testOverallAssessment
        .put(nodeKey, mockAssessment);

    testAssessor.undoActionImpactOnOverallAssessment(mockAction, testOverallAssessment);

    verify(mockAssessment, times(0)).removeActionImpact(anyString(), any());
  }
}
