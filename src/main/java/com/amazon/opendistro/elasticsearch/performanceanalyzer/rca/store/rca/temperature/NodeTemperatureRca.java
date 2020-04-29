/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.CompactNodeTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.DimensionalTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.FullNodeTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.NodeLevelDimensionalSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.CpuUtilDimensionTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.HeapAllocRateTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeTemperatureRca extends Rca<CompactNodeTemperatureFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(NodeTemperatureRca.class);
  private final CpuUtilDimensionTemperatureRca cpuUtilDimensionTemperatureRca;
  private final HeapAllocRateTemperatureRca heapAllocRateTemperatureRca;

  public NodeTemperatureRca(CpuUtilDimensionTemperatureRca cpuUtilDimensionTemperatureRca,
      HeapAllocRateTemperatureRca heapAllocRateTemperatureRca) {
    super(5);
    this.cpuUtilDimensionTemperatureRca = cpuUtilDimensionTemperatureRca;
    this.heapAllocRateTemperatureRca = heapAllocRateTemperatureRca;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitMessage> flowUnitMessages =
        args.getWireHopper().readFromWire(args.getNode());
    List<CompactNodeTemperatureFlowUnit> flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
      flowUnitList.add(CompactNodeTemperatureFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
    }
    setFlowUnits(flowUnitList);
  }

  /**
   * The goal of the NodeHeatRca is to build a node level temperature profile.
   *
   * <p>This is done by accumulating the {@code DimensionalFlowUnit} s it receives from the
   * individual ResourceHeatRcas. The temperature profile build here is sent to the elected master
   * node where this is used to calculate the cluster temperature profile.
   *
   * @return
   */
  @Override
  public CompactNodeTemperatureFlowUnit operate() {
    // TODO: Make this process a list of dimensions instead of writing them out and add the
    //  processed dimension profiles to the summary.
    List<DimensionalTemperatureFlowUnit> cpuFlowUnits = cpuUtilDimensionTemperatureRca
        .getFlowUnits();
    List<DimensionalTemperatureFlowUnit> heapAllocRateFlowUnits = heapAllocRateTemperatureRca
        .getFlowUnits();
    // EachResourceLevelHeat RCA should generate a one @{code DimensionalFlowUnit}.
    if (cpuFlowUnits.size() != 1) {
      throw new IllegalArgumentException("One flow unit expected. Found: " + cpuFlowUnits);
    }

    if (heapAllocRateFlowUnits.size() != 1) {
      throw new IllegalStateException("One flow unit expected. Found: " + heapAllocRateFlowUnits);
    }

    // This means that the input RCA didn't calculate anything. We can move on as well.
    if (cpuFlowUnits.get(0).isEmpty() && heapAllocRateFlowUnits.get(0).isEmpty()) {
      return new CompactNodeTemperatureFlowUnit(System.currentTimeMillis());
    }

    List<NodeLevelDimensionalSummary> nodeDimensionProfiles = new ArrayList<>();
    if (!cpuFlowUnits.get(0).isEmpty()) {
      nodeDimensionProfiles.add(cpuFlowUnits.get(0).getNodeDimensionProfile());
    }

    if (!heapAllocRateFlowUnits.get(0).isEmpty()) {
      nodeDimensionProfiles.add(heapAllocRateFlowUnits.get(0).getNodeDimensionProfile());
    }
    FullNodeTemperatureSummary nodeProfile = buildNodeProfile(nodeDimensionProfiles);

    ResourceContext resourceContext = new ResourceContext(Resources.State.UNKNOWN);
    CompactNodeSummary summary = new CompactNodeSummary(nodeProfile.getNodeId(),
        nodeProfile.getHostAddress());
    summary.fillFromNodeProfile(nodeProfile);

    return new CompactNodeTemperatureFlowUnit(
        System.currentTimeMillis(), resourceContext,
        summary,
        true);
  }

  private FullNodeTemperatureSummary buildNodeProfile(
      List<NodeLevelDimensionalSummary> dimensionProfiles) {
    ClusterDetailsEventProcessor.NodeDetails currentNodeDetails =
        ClusterDetailsEventProcessor.getCurrentNodeDetails();
    FullNodeTemperatureSummary nodeProfile = new FullNodeTemperatureSummary(
        currentNodeDetails.getId(),
        currentNodeDetails.getHostAddress());
    for (NodeLevelDimensionalSummary profile : dimensionProfiles) {
      nodeProfile.updateNodeDimensionProfile(profile);
    }
    return nodeProfile;
  }
}
