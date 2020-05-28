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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HotNodeRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {

  private static final Logger LOG = LogManager.getLogger(HotNodeRca.class);
  private Rca<ResourceFlowUnit<HotResourceSummary>>[] hotResourceRcas;
  private boolean hasUnhealthyFlowUnit;
  // the amount of RCA period this RCA needs to run before sending out a flowunit
  private final int rcaPeriod;
  private int counter;

  public <R extends Rca<ResourceFlowUnit<HotResourceSummary>>> HotNodeRca(final int rcaPeriod, R... hotResourceRcas) {
    super(5);
    this.hotResourceRcas = hotResourceRcas.clone();
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    hasUnhealthyFlowUnit = false;
  }

  public <R extends Rca<ResourceFlowUnit<HotResourceSummary>>> HotNodeRca(final int rcaPeriod, Collection<R> hotResourceRcas) {
    super(5);
    this.hotResourceRcas = hotResourceRcas.toArray(new Rca[hotResourceRcas.size()]);
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    hasUnhealthyFlowUnit = false;
  }

  @Override
  public ResourceFlowUnit<HotNodeSummary> operate() {
    counter++;
    List<HotResourceSummary> hotResourceSummaryList = new ArrayList<>();
    for (int i = 0; i < hotResourceRcas.length; i++) {
      final List<ResourceFlowUnit<HotResourceSummary>> hotResourceFlowUnits = hotResourceRcas[i].getFlowUnits();
      for (final ResourceFlowUnit<HotResourceSummary> hotResourceFlowUnit : hotResourceFlowUnits) {
        if (hotResourceFlowUnit.isEmpty()) {
          continue;
        }
        if (hotResourceFlowUnit.hasSummary()) {
          hotResourceSummaryList.add(hotResourceFlowUnit.getSummary());
        }
        if (hotResourceFlowUnit.getResourceContext().isUnhealthy()) {
          hasUnhealthyFlowUnit = true;
        }
      }
    }

    if (counter == rcaPeriod) {
      ResourceContext context;
      ClusterDetailsEventProcessor.NodeDetails currentNode = ClusterDetailsEventProcessor
          .getCurrentNodeDetails();
      HotNodeSummary summary = new HotNodeSummary(currentNode.getId(), currentNode.getHostAddress());

      for (HotResourceSummary resourceSummary : hotResourceSummaryList) {
        summary.appendNestedSummary(resourceSummary);
      }

      if (hasUnhealthyFlowUnit) {
        context = new ResourceContext(Resources.State.UNHEALTHY);
      } else {
        context = new ResourceContext(Resources.State.HEALTHY);
      }

      // reset the variables
      counter = 0;
      hasUnhealthyFlowUnit = false;
      //check if the current node is data node. If it is the data node
      //then HotNodeRca is the top level RCA on this node and we want to persist summaries in flowunit.
      boolean isDataNode = !currentNode.getIsMasterNode();
      return new ResourceFlowUnit<>(System.currentTimeMillis(), context, summary, isDataNode);
    } else {
      return new ResourceFlowUnit<>(System.currentTimeMillis());
    }
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitMessage> flowUnitMessages =
        args.getWireHopper().readFromWire(args.getNode());
    List<ResourceFlowUnit<HotNodeSummary>> flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitMessage message : flowUnitMessages) {
      ResourceFlowUnit<HotNodeSummary> flowUnit = null;
      //if the flowunit is empty. empty flowunit does not have context
      if (message.hasResourceContext()) {
        ResourceContext newContext = ResourceContext
            .buildResourceContextFromMessage(message.getResourceContext());
        HotNodeSummary newSummary = null;
        if (message.getSummaryOneofCase().getNumber()
            == FlowUnitMessage.SummaryOneofCase.HOTNODESUMMARY.getNumber()
            && message.hasHotNodeSummary()) {
          newSummary = HotNodeSummary.buildHotNodeSummaryFromMessage(message.getHotNodeSummary());
        }
        flowUnit = new ResourceFlowUnit<>(message.getTimeStamp(), newContext, newSummary);
      } else {
        //empty flowunit;
        //TODO: we might not want to send empty flowunit across network.
        flowUnit = new ResourceFlowUnit<>(message.getTimeStamp());
      }
      flowUnitList.add(flowUnit);
    }
    setFlowUnits(flowUnitList);
  }
}
