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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Rca<T extends ResourceFlowUnit> extends NonLeafNode<T> {
  private static final Logger LOG = LogManager.getLogger(Rca.class);
  // the amount of RCA period this RCA needs to run before sending out a flowunit
  protected final int rcaPeriod;
  protected boolean alwaysCreateSummary;
  protected int counter;

  public Rca(long evaluationIntervalSeconds, int rcaPeriod) {
    super(0, evaluationIntervalSeconds);
    this.rcaPeriod = rcaPeriod;
    this.alwaysCreateSummary = false;
    counter = 0;
  }

  /**
   * set the alwaysCreateSummary
   * @param alwaysCreateSummary if alwaysCreateSummary is true, the RCA will always create a summary
   *                            for this flowunit regardless of its state(whether healthy or unhealthy)
   *
   */
  public void alwaysCreateSummary(boolean alwaysCreateSummary) {
    this.alwaysCreateSummary = alwaysCreateSummary;
  }
  
  /**
   * fetch flowunits from local graph node
   * @param args The wrapper around the flow unit operation.
   */
  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("rca: Executing fromLocal: {}", this.getClass().getSimpleName());
    setFlowUnits(Collections.singletonList(this.operate()));
  }

  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
    for (final T flowUnit : getFlowUnits()) {
        args.getPersistable().write(this, flowUnit);
    }
  }
}
