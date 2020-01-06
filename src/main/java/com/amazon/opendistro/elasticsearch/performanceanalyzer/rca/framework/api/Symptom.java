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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Symptom extends NonLeafNode<SymptomFlowUnit> {
  private static final Logger LOG = LogManager.getLogger(Symptom.class);

  public Symptom(long evaluationIntervalSeconds) {
    super(0, evaluationIntervalSeconds);
  }

  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("rca: Executing handleRca: {}", this.getClass().getSimpleName());
    setFlowUnits(Collections.singletonList(this.operate()));
  }

  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    // TODO
  }

  /**
   * Persists a flow unit.
   * @param args The arg wrapper.
   */
  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
  }
}
