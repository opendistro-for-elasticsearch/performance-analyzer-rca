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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HotNodeRca extends Rca<ResourceFlowUnit> {

  private static final Logger LOG = LogManager.getLogger(HotNodeRca.class);
  private static final int RCA_PERIOD = 12;
  private Rca[] hotResourceRcas;
  private int counter;

  public <R extends Rca> HotNodeRca(final long evaluationIntervalSeconds, R... hotResourceRcas) {
    super(evaluationIntervalSeconds);
    this.hotResourceRcas = hotResourceRcas.clone();
    counter = 0;
  }

  public <R extends Rca> HotNodeRca(final long evaluationIntervalSeconds,
      Collection<R> hotResourceRcas) {
    super(evaluationIntervalSeconds);
    this.hotResourceRcas = hotResourceRcas.toArray(new Rca[hotResourceRcas.size()]);
    counter = 0;
  }

  @Override
  public ResourceFlowUnit operate() {
    boolean shouldReportOperation = false;
    counter++;
    List<GenericSummary> hotResourceSummaryList = new ArrayList<>();
    for (int i = 0; i < hotResourceRcas.length; i++) {
      final List<ResourceFlowUnit> hotResourceFlowUnits = hotResourceRcas[i].getFlowUnits();
      for (final ResourceFlowUnit hotResourceFlowUnit : hotResourceFlowUnits) {
        if (hotResourceFlowUnit.isEmpty()) {
          continue;
        }
        if (hotResourceFlowUnit.getResourceContext().isUnhealthy()) {
          hotResourceSummaryList.add(hotResourceFlowUnit.getResourceSummary());
          shouldReportOperation = true;
        }
        //TODO : testing purpose. remove later
        /*
        else {
          hotResourceSummaryList.add(hotResourceFlowUnit.getResourceSummary());
          shouldReportOperation = true;
        }
        */
      }
    }

    if (counter == RCA_PERIOD) {
      ResourceContext context = null;
      HotNodeSummary summary = null;
      // reset the variables
      counter = 0;
      if (shouldReportOperation) {
        LOG.info("@RCA: [Hot Node] ");
        context = new ResourceContext(Resources.State.UNHEALTHY);
        ClusterDetailsEventProcessor.NodeDetails currentNode = ClusterDetailsEventProcessor
            .getCurrentNodeDetails();
        summary = new HotNodeSummary(currentNode.getId(), currentNode.getHostAddress());
        summary.setNestedSummaryList(hotResourceSummaryList);
      } else {
        context = new ResourceContext(Resources.State.HEALTHY);
      }
      return new ResourceFlowUnit(System.currentTimeMillis(), context, summary);
    } else {
      return new ResourceFlowUnit(System.currentTimeMillis());
    }
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitMessage> flowUnitMessages =
        args.getWireHopper().readFromWire(args.getNode());
    List<ResourceFlowUnit> flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
      flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
    }
    setFlowUnits(flowUnitList);
  }
}
