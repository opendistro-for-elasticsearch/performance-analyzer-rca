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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a generic cluster level RCA which subscripts a single upstream vertex HotNodeRca.
 * This cluster RCA maintains a two dimensional table indexed by (NodeId, ResourceType). And within
 * each cell, it keeps track of the last three consecutive resource summaries and will mark the node
 * as unhealthy if all three resource summaries received from that node are unhealthy,
 */
public class GenericClusterRca extends Rca<ResourceFlowUnit> {
  private static final Logger LOG = LogManager.getLogger(GenericClusterRca.class);
  private static final int UNHEALTHY_FLOWUNIT_THRESHOLD = 3;
  private static final long TIMESTAMP_EXPIRATION_IN_MINS = 5;
  private final Rca<ResourceFlowUnit> hotNodeRca;
  private final HashSet<ResourceType> resourceTypes;
  private final Table<String, ResourceType, ImmutableList<ResourceSummaryWrapper>> nodeTable;
  private final int rcaPeriod;
  private int counter;
  private List<NodeDetails> dataNodesDetails;
  protected Clock clock;

  public <R extends Rca> GenericClusterRca(final int rcaPeriod,
      final R hotNodeRca, final List<ResourceType> resourceTypes) {
    super(5);
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    this.clock = Clock.systemUTC();
    this.hotNodeRca = hotNodeRca;
    this.resourceTypes = new HashSet<>(resourceTypes);
    nodeTable = HashBasedTable.create();
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  //add Resource Summary to the corresponding cell in NodeTable
  private void addSummaryToNodeMap(final String nodeId, final ResourceType resourceType, ResourceSummaryWrapper summaryWrapper) {
    LinkedList<ResourceSummaryWrapper> linkedList = new LinkedList<>();
    ImmutableList<ResourceSummaryWrapper> oldUsage = nodeTable.get(nodeId, resourceType);
    if (oldUsage != null) {
      linkedList.addAll(oldUsage);
    }
    linkedList.addLast(summaryWrapper);
    while (!linkedList.isEmpty()) {
      if (TimeUnit.MILLISECONDS.toMinutes(summaryWrapper.timestamp - linkedList.getFirst().timestamp)
              > TIMESTAMP_EXPIRATION_IN_MINS) {
        linkedList.pollFirst();
      }
      else {
        break;
      }
    }
    while (linkedList.size() > UNHEALTHY_FLOWUNIT_THRESHOLD) {
      linkedList.pollFirst();
    }
    nodeTable.put(nodeId, resourceType, ImmutableList.copyOf(linkedList));
  }

  //parse flowunit
  private void parseFlowunit(List<ResourceFlowUnit> hotNodeRcaFlowUnits) {
    for (ResourceFlowUnit hotNodeRcaFlowUnit : hotNodeRcaFlowUnits) {
      if (hotNodeRcaFlowUnit.isEmpty()) {
        continue;
      }
      if (!(hotNodeRcaFlowUnit.getResourceSummary() instanceof HotNodeSummary)) {
        LOG.error("RCA : Receive flowunit from unexpected rca node");
        continue;
      }
      HotNodeSummary nodeSummary = (HotNodeSummary) hotNodeRcaFlowUnit.getResourceSummary();

      long timestamp = clock.millis();
      HashSet<ResourceType> unhealthyResourceType = new HashSet<>();
      if (hotNodeRcaFlowUnit.getResourceContext().isUnhealthy() && nodeSummary.getNestedSummaryList() != null) {
        for (GenericSummary summary : nodeSummary.getNestedSummaryList()) {
          if (summary instanceof HotResourceSummary) {
            HotResourceSummary resourceSummary = (HotResourceSummary) summary;
            ResourceType currResourceType = resourceSummary.getResourceType();
            if (resourceTypes.contains(currResourceType)) {
              addSummaryToNodeMap(nodeSummary.getNodeID(), currResourceType,
                  new ResourceSummaryWrapper(timestamp, false, resourceSummary));
              unhealthyResourceType.add(currResourceType);
            }
          } else {
            LOG.error("RCA : unexpected summary type !");
          }
        }
      }

      for (ResourceType currResourceType : resourceTypes) {
        if (!unhealthyResourceType.contains(currResourceType)) {
          addSummaryToNodeMap(nodeSummary.getNodeID(), currResourceType,
              new ResourceSummaryWrapper(timestamp, true, null));
        }
      }
    }
  }

  //TODO : we might need to change this function later to use EventListener
  // to update the nodeMap whenever the ClusterDetailsEventProcessor is updated
  // so we don't have to keep polling the NodeDetails in every time window.
  private void removeInactiveNodeFromTable() {
    Set<String> nodeIdSet = new HashSet<>();
    for (NodeDetails nodeDetail : dataNodesDetails) {
      nodeIdSet.add(nodeDetail.getId());
    }
    for (String nodeId : nodeTable.rowKeySet()) {
      if (!nodeIdSet.contains(nodeId)) {
        nodeTable.row(nodeId).clear();
      }
    }
  }

  protected ResourceFlowUnit generateFlowunit() {
    List<HotNodeSummary> unhealthyNodes = new ArrayList<>();
    for (NodeDetails nodeDetails : dataNodesDetails) {

      List<HotResourceSummary> unhealthyResources = new ArrayList<>();
      for (ResourceType resourceType : resourceTypes) {
        ImmutableList<ResourceSummaryWrapper> resourceSummaryWrappers =
            nodeTable.get(nodeDetails.getId(), resourceType);
        if (resourceSummaryWrappers != null) {
          int cnt = 0;
          for (ResourceSummaryWrapper wrapper : resourceSummaryWrappers) {
            if (!wrapper.healthy) {
              cnt++;
            }
          }
          if (cnt >= UNHEALTHY_FLOWUNIT_THRESHOLD) {
            unhealthyResources.add(resourceSummaryWrappers.get(0).resourceSummary);
          }
        }
      }

      if (!unhealthyResources.isEmpty()) {
        HotNodeSummary nodeSummary = new HotNodeSummary(nodeDetails.getId(), nodeDetails.getHostAddress());
        nodeSummary.addNestedSummaryList(unhealthyResources);
        unhealthyNodes.add(nodeSummary);
      }
    }

    if (!unhealthyNodes.isEmpty()) {
      HotClusterSummary clusterSummary = new HotClusterSummary(dataNodesDetails.size(), unhealthyNodes.size());
      clusterSummary.addNestedSummaryList(unhealthyNodes);
      return new ResourceFlowUnit(clock.millis(), new ResourceContext(Resources.State.UNHEALTHY), clusterSummary, true);
    }
    else {
      return new ResourceFlowUnit(clock.millis(), new ResourceContext(State.HEALTHY), null, true);
    }
  }

  @Override
  public ResourceFlowUnit operate() {
    dataNodesDetails = ClusterDetailsEventProcessor.getDataNodesDetails();

    counter += 1;
    parseFlowunit(hotNodeRca.getFlowUnits());

    if (counter >= rcaPeriod) {
      counter = 0;
      removeInactiveNodeFromTable();
      return generateFlowunit();
    } else {
      return new ResourceFlowUnit(System.currentTimeMillis());
    }
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
        + "be required.");
  }

  /**
   * a wrapper to add local timestamp to HotResourceSummary
   */
  private static class ResourceSummaryWrapper {
    private final long timestamp;
    private final HotResourceSummary resourceSummary;
    private boolean healthy;

    ResourceSummaryWrapper(final long timestamp, final boolean healthy, final HotResourceSummary resourceSummary) {
      this.timestamp = timestamp;
      this.healthy = healthy;
      this.resourceSummary = resourceSummary;
    }
  }
}
