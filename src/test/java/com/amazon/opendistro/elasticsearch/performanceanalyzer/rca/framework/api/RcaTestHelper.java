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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotShardSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RcaTestHelper<T extends GenericSummary> extends Rca<ResourceFlowUnit<T>> {
  public RcaTestHelper() {
    super(5);
  }

  public void mockFlowUnit(ResourceFlowUnit<T> flowUnit) {
    this.flowUnits = Collections.singletonList(flowUnit);
  }

  public void mockFlowUnits(List<ResourceFlowUnit<T>> flowUnitList) {
    this.flowUnits = flowUnitList;
  }

  @Override
  public ResourceFlowUnit<T> operate() {
    return null;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
  }

  public static ResourceFlowUnit<HotNodeSummary> generateFlowUnit(ResourceType type, String nodeID, Resources.State healthy) {
    HotResourceSummary resourceSummary = new HotResourceSummary(type,
        10, 5, 60);
    HotNodeSummary nodeSummary = new HotNodeSummary(nodeID, "127.0.0.0");
    nodeSummary.appendNestedSummary(resourceSummary);
    return new ResourceFlowUnit<>(System.currentTimeMillis(), new ResourceContext(healthy), nodeSummary);
  }

  public static ResourceFlowUnit<HotNodeSummary> generateFlowUnitForHotShard(String indexName, String shardId, String nodeID,
      double cpu_utilization, double io_throughput, double io_sys_callrate, Resources.State health) {
    HotShardSummary hotShardSummary = new HotShardSummary(indexName, shardId, nodeID, 60);
    hotShardSummary.setcpuUtilization(cpu_utilization);
    hotShardSummary.setCpuUtilizationThreshold(0.50);
    hotShardSummary.setIoThroughput(io_throughput);
    hotShardSummary.setIoThroughputThreshold(500000);
    hotShardSummary.setIoSysCallrate(io_sys_callrate);
    hotShardSummary.setIoSysCallrateThreshold(0.50);
    HotNodeSummary nodeSummary = new HotNodeSummary(nodeID, "127.0.0.0");
    nodeSummary.appendNestedSummary(hotShardSummary);
    return new ResourceFlowUnit<>(System.currentTimeMillis(), new ResourceContext(health), nodeSummary);
  }
}
