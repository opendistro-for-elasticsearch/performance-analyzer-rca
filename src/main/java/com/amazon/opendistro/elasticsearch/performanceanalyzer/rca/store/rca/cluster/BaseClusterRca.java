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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This is a generic cluster level RCA which subscripts a single upstream node level RCA.
 * This cluster RCA maintains a hashtable to keep track of flowunits sending from different nodes across
 * the cluster. This RCA will mark the cluster as unhealthy if the flowunits from any data nodes are unhealthy.
 * <p></p>
 * A few protected variables that can be overridden by derived class:
 * numOfFlowUnitsInMap : number of consecutive flowunits stored in hashtable. Default is 1
 * collectFromMasterNode : whether this RCA collect flowunit from master nodes.
 * expirationTimeWindow : time window to determine whether flowunit in hashmap becomes stale
 */
public class BaseClusterRca extends Rca<ResourceFlowUnit<HotClusterSummary>> {
  private static final int DEFAULT_NUM_OF_FLOWUNITS = 1;
  private static final long TIMESTAMP_EXPIRATION_IN_MILLIS = TimeUnit.MINUTES.toMillis(10);
  private final Rca<ResourceFlowUnit<HotNodeSummary>> nodeRca;
  private final Map<NodeKey, LinkedList<ResourceFlowUnit<HotNodeSummary>>> nodeMap;
  private final int rcaPeriod;
  private int counter;
  protected Clock clock;
  protected int numOfFlowUnitsInMap;
  protected boolean collectFromMasterNode;
  protected long expirationTimeWindow;


  public <R extends Rca<ResourceFlowUnit<HotNodeSummary>>> BaseClusterRca(final int rcaPeriod,
      final R nodeRca) {
    super(5);
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    this.clock = Clock.systemUTC();
    this.nodeRca = nodeRca;
    this.numOfFlowUnitsInMap = DEFAULT_NUM_OF_FLOWUNITS;
    this.nodeMap = new HashMap<>();
    this.collectFromMasterNode = false;
    this.expirationTimeWindow = TIMESTAMP_EXPIRATION_IN_MILLIS;
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @VisibleForTesting
  public void setCollectFromMasterNode(boolean collectFromMasterNode) {
    this.collectFromMasterNode = collectFromMasterNode;
  }

  //add upstream flowunits collected from different nodes into hashmap
  private void addUpstreamFlowUnits(List<ResourceFlowUnit<HotNodeSummary>> flowUnits) {
    for (ResourceFlowUnit<HotNodeSummary> flowUnit : flowUnits) {
      if (flowUnit.isEmpty()) {
        continue;
      }
      HotNodeSummary nodeSummary = flowUnit.getSummary();
      NodeKey nodeKey = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());

      if (!nodeMap.containsKey(nodeKey)) {
        nodeMap.put(nodeKey, new LinkedList<>());
      }
      LinkedList<ResourceFlowUnit<HotNodeSummary>> linkedList = nodeMap.get(nodeKey);
      linkedList.addLast(flowUnit);
      if (linkedList.size() > numOfFlowUnitsInMap) {
        linkedList.pollFirst();
      }
    }
  }

  private List<NodeDetails> getClusterNodesDetails() {
    if (collectFromMasterNode) {
      return ClusterDetailsEventProcessor.getNodesDetails();
    }
    else {
      return ClusterDetailsEventProcessor.getDataNodesDetails();
    }
  }

  //TODO : we might need to change this function later to use EventListener
  // to update the nodeMap whenever the ClusterDetailsEventProcessor is updated
  // so we don't have to keep polling the NodeDetails in every time window.
  private void removeInactiveNodeFromNodeMap() {
    Set<String> nodeIdSet = new HashSet<>();
    for (NodeDetails nodeDetail : getClusterNodesDetails()) {
      nodeIdSet.add(nodeDetail.getId());
    }
    nodeMap.entrySet().removeIf(entry -> !nodeIdSet.contains(entry.getKey().getNodeId()));
  }

  /**
   * generate flowunit for downstream based on the flowunits this RCA collects in hashmap
   * flowunits with timestamp beyond expirationTimeWindow time frame are  considered
   * as stale and ignored by this RCA.
   * @return flowunit for downstream vertices
   */
  protected ResourceFlowUnit<HotClusterSummary> generateFlowUnit() {
    List<HotNodeSummary> unhealthyNodeSummaries = new ArrayList<>();
    long timestamp = clock.millis();
    List<NodeDetails> clusterNodesDetails = getClusterNodesDetails();
    for (NodeDetails nodeDetails : clusterNodesDetails) {
      NodeKey nodeKey = new NodeKey(nodeDetails.getId(), nodeDetails.getHostAddress());
      if (nodeMap.containsKey(nodeKey)) {
        ResourceFlowUnit<HotNodeSummary> flowUnit = nodeMap.get(nodeKey).getLast();
        if (timestamp - flowUnit.getTimeStamp() <= TIMESTAMP_EXPIRATION_IN_MILLIS
            && flowUnit.getResourceContext().isUnhealthy()) {
          unhealthyNodeSummaries.add(flowUnit.getSummary());
        }
      }
    }

    if (!unhealthyNodeSummaries.isEmpty()) {
      HotClusterSummary clusterSummary = new HotClusterSummary(clusterNodesDetails.size(), unhealthyNodeSummaries.size());
      for (HotNodeSummary nodeSummary : unhealthyNodeSummaries) {
        clusterSummary.appendNestedSummary(nodeSummary);
      }
      return new ResourceFlowUnit<>(timestamp, new ResourceContext(Resources.State.UNHEALTHY), clusterSummary, true);
    }
    else {
      return new ResourceFlowUnit<>(timestamp, new ResourceContext(State.HEALTHY), null);
    }
  }

  @Override
  public ResourceFlowUnit<HotClusterSummary> operate() {
    counter += 1;
    addUpstreamFlowUnits(nodeRca.getFlowUnits());

    if (counter >= rcaPeriod) {
      counter = 0;
      removeInactiveNodeFromNodeMap();
      return generateFlowUnit();
    } else {
      return new ResourceFlowUnit<>(System.currentTimeMillis());
    }
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
        + "be required.");
  }
}
