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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Publisher extends NonLeafNode<EmptyFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(Publisher.class);

  private Collator collator;
  private boolean isMuted = false;

  public Publisher(int evalIntervalSeconds, Collator collator) {
    super(0, evalIntervalSeconds);
    this.collator = collator;
  }

  @Override
  public EmptyFlowUnit operate() {
    // TODO: Pass through implementation, need to add dampening, cool-off, action flip-flop
    // avoidance, state persistence etc.

    Decision decision = collator.getFlowUnits().get(0);
    for (Action action : decision.getActions()) {
      LOG.info("Executing action: [{}]", action.name());
      action.execute();
    }

    return new EmptyFlowUnit(System.currentTimeMillis());
  }

  /* Publisher does not have downstream nodes and does not emit flow units
   */

  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    assert true;
  }

  @Override
  public void handleNodeMuted() {
    assert true;
  }
}
