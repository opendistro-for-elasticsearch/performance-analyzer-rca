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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.Collections;

public class RcaTestHelper extends Rca<ResourceFlowUnit> {
  public RcaTestHelper() {
    super(5);
  }

  public void mockFlowUnit(ResourceFlowUnit flowUnit) {
    this.flowUnits = Collections.singletonList(flowUnit);
  }

  @Override
  public ResourceFlowUnit operate() {
    return null;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
  }

  public static ResourceFlowUnit generateFlowUnit(ResourceType type, String nodeID, Resources.State healthy) {
    HotResourceSummary resourceSummary = new HotResourceSummary(type,
        10, 5, 60);
    HotNodeSummary nodeSummary = new HotNodeSummary(nodeID, "127.0.0.0");
    nodeSummary.addNestedSummaryList(resourceSummary);
    return new ResourceFlowUnit(System.currentTimeMillis(), new ResourceContext(healthy), nodeSummary);
  }
}
