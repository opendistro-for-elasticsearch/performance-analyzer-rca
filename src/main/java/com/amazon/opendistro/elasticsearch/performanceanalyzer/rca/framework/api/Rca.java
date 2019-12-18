/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Rca extends NonLeafNode {
  private static final Logger LOG = LogManager.getLogger(Rca.class);
  protected List<ResourceFlowUnit> flowUnitList;

  public Rca(long evaluationIntervalSeconds) {
    super(0, evaluationIntervalSeconds);
  }

  public List<ResourceFlowUnit> fetchFlowUnitList() {
    return this.flowUnitList;
  }

  public void setGernericFlowUnitList() {
    this.flowUnitList = Collections.singletonList(ResourceFlowUnit.generic());
  }

  // TODO: change the name as it calls itself generate but secretly persists the flow unit.
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    LOG.debug("rca: Executing fromLocal: {}", this.getClass().getSimpleName());
    this.flowUnitList = Collections.singletonList(this.operate());
    for (final ResourceFlowUnit flowUnit : flowUnitList) {
      if (!flowUnit.isEmpty() && flowUnit.getData() != null && !flowUnit.getData().isEmpty()) {
        args.getPersistable().write(this, flowUnit);
      }
    }
  }

  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitWrapper> flowUnitWrappers =
        args.getWireHopper().readFromWire(args.getNode());
    flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitWrapper messageWrapper : flowUnitWrappers) {
      flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(messageWrapper));
    }
  }
}
