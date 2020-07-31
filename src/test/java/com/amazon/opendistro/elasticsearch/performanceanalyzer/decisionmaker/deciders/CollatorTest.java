/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class CollatorTest {

  private static final long DUMMY_EVAL_INTERVAL = 1;
  private static final String MUTED_ACTION_NAME = "muted action";
  private static final String UNMUTED_ACTION_NAME = "unmuted action";
  private Collator testCollator;
  private Action mutedAction = new DummyAction(MUTED_ACTION_NAME);
  private Action unmutedAction = new DummyAction(UNMUTED_ACTION_NAME);

  @Mock
  Decider mockDecider1;

  @Mock
  Decider mockDecider2;

  @Before
  public void setup() {
    initMocks(this);
    this.testCollator = new Collator(DUMMY_EVAL_INTERVAL, mockDecider1, mockDecider2);
  }

  @Test
  public void testFilterMutedActions() {
    Stats.getInstance().updateMutedActions(ImmutableSet.of(mutedAction.name()));
    Decision mutedDecision = new Decision(System.currentTimeMillis(), "decision1");
    mutedDecision.addAction(mutedAction);

    Decision unmutedDecision = new Decision(System.currentTimeMillis(), "decision2");
    unmutedDecision.addAction(unmutedAction);
    when(mockDecider1.getFlowUnits()).thenReturn(Collections.singletonList(mutedDecision));
    when(mockDecider2.getFlowUnits()).thenReturn(Collections.singletonList(unmutedDecision));

    final Decision collatorDecision = testCollator.operate();

    assertEquals(1, collatorDecision.getActions().size());
    assertEquals(unmutedAction.name(), collatorDecision.getActions().get(0).name());
  }

  private static class DummyAction implements Action {

    private final String name;

    public DummyAction(final String name) {
      this.name = name;
    }

    /**
     * Returns true if the configured action is actionable, false otherwise.
     *
     * <p>Examples of non-actionable actions are resource configurations where limits have been
     * reached.
     */
    @Override
    public boolean isActionable() {
      return false;
    }

    /**
     * Time to wait since last recommendation, before suggesting this action again
     */
    @Override
    public long coolOffPeriodInMillis() {
      return 0;
    }

    /**
     * Returns a list of Elasticsearch nodes impacted by this action.
     */
    @Override
    public List<NodeKey> impactedNodes() {
      return null;
    }

    /**
     * Returns a map of Elasticsearch nodes to ImpactVector of this action on that node
     */
    @Override
    public Map<NodeKey, ImpactVector> impact() {
      return null;
    }

    /**
     * Returns action name
     */
    @Override
    public String name() {
      return name;
    }

    /**
     * Returns a summary for the configured action
     */
    @Override
    public String summary() {
      return null;
    }
  }
}