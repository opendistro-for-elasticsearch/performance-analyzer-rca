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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.StaticBucketThresholds;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.UsageBucketThresholds;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a generic cluster level RCA which subscripts upstream node level RCAs and generate a flowunit
 * with cluster level summary that concludes the healthiness of the cluster in terms of those node level RCAs.
 * This cluster RCA maintains a Table to keep track of flowunits sending from different nodes across
 * the cluster. This table is a two dimensional table indexed by (NodeKey, Rca Name) and each cells stores
 * that last numOfFlowUnitsInMap flowunits it receives. This RCA will
 * mark the cluster as unhealthy if the flowunits from any data nodes are unhealthy.
 * <p></p>
 * A few protected variables that can be overridden by derived class:
 * numOfFlowUnitsInMap : number of consecutive flowunits stored in hashtable. Default is 1
 * collectFromMasterNode : whether this RCA collect flowunit from master nodes.
 * expirationTimeWindow : time window to determine whether flowunit in hashmap becomes stale
 * method that can be overriden :
 * generateNodeSummary(NodeKey) : how do we want to parse the table and generate summary for one node.
 */
public class BaseClusterRca extends Rca<ResourceFlowUnit<HotClusterSummary>> {

  private static final Logger LOG = LogManager.getLogger(BaseClusterRca.class);
  private static final int DEFAULT_NUM_OF_FLOWUNITS = 1;
  private static final long TIMESTAMP_EXPIRATION_IN_MILLIS = TimeUnit.MINUTES.toMillis(10);
  protected final List<Rca<ResourceFlowUnit<HotNodeSummary>>> nodeRcas;
  // two dimensional table indexed by (NodeKey, Rca Name) => last numOfFlowUnitsInMap flowunits
  protected final Table<NodeKey, String, LinkedList<ResourceFlowUnit<HotNodeSummary>>> nodeTable;
  private final int rcaPeriod;
  private int counter;
  protected Clock clock;
  protected int numOfFlowUnitsInMap;
  protected boolean collectFromMasterNode;
  protected long expirationTimeWindow;
  protected boolean computeUsageBuckets;
  protected UsageBucketThresholds usageBucketThresholds;


  @SafeVarargs
  public <R extends Rca<ResourceFlowUnit<HotNodeSummary>>> BaseClusterRca(final int rcaPeriod,
      final R... nodeRca) {
    super(5);
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    this.clock = Clock.systemUTC();
    this.numOfFlowUnitsInMap = DEFAULT_NUM_OF_FLOWUNITS;
    this.nodeTable = HashBasedTable.create();
    this.collectFromMasterNode = false;
    this.computeUsageBuckets = false;
    this.expirationTimeWindow = TIMESTAMP_EXPIRATION_IN_MILLIS;
    this.nodeRcas = Arrays.asList(nodeRca);
    this.usageBucketThresholds = new StaticBucketThresholds();
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @VisibleForTesting
  public void setCollectFromMasterNode(boolean collectFromMasterNode) {
    this.collectFromMasterNode = collectFromMasterNode;
  }

  //add upstream flowunits collected from different nodes into Table
  private void addUpstreamFlowUnits(Rca<ResourceFlowUnit<HotNodeSummary>> nodeRca) {
    List<ResourceFlowUnit<HotNodeSummary>> flowUnits = nodeRca.getFlowUnits();
    for (ResourceFlowUnit<HotNodeSummary> flowUnit : flowUnits) {
      if (flowUnit.isEmpty() || !flowUnit.hasResourceSummary()) {
        continue;
      }
      HotNodeSummary nodeSummary = flowUnit.getSummary();
      NodeKey nodeKey = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());

      if (nodeTable.get(nodeKey, nodeRca.name()) == null) {
        nodeTable.put(nodeKey, nodeRca.name(), new LinkedList<>());
      }
      LinkedList<ResourceFlowUnit<HotNodeSummary>> linkedList = nodeTable.get(nodeKey, nodeRca.name());
      linkedList.addLast(flowUnit);
      if (linkedList.size() > numOfFlowUnitsInMap) {
        linkedList.pollFirst();
      }
    }
  }

  private List<InstanceDetails> getClusterNodesDetails() {
    if (collectFromMasterNode) {
      return getAllClusterInstances();
    }
    else {
      return getDataNodeInstances();
    }
  }

  // TODO : we might need to change this function later to use EventListener
  // to update the nodeMap whenever the ClusterDetailsEventProcessor is updated
  // so we don't have to keep polling the NodeDetails in every time window.
  private void removeInactiveNodeFromNodeMap() {
    Set<InstanceDetails.Id> nodeIdSet = new HashSet<>();
    List<NodeKey> inactiveNodes = new ArrayList<>();
    for (InstanceDetails nodeDetail : getClusterNodesDetails()) {
      nodeIdSet.add(nodeDetail.getInstanceId());
    }
    for (NodeKey nodeKey : nodeTable.rowKeySet()) {
      if (!nodeIdSet.contains(nodeKey.getNodeId())) {
        inactiveNodes.add(nodeKey);
        LOG.info("RCA: remove node {} from node map", nodeKey);
      }
    }
    inactiveNodes.forEach(nodeKey -> nodeTable.row(nodeKey).clear());
  }

  /**
   * generate flowunit for downstream based on the flowunits this RCA collects in hashmap
   * flowunits with timestamp beyond expirationTimeWindow time frame are  considered
   * as stale and ignored by this RCA.
   * @return flowunit for downstream vertices
   */
  private ResourceFlowUnit<HotClusterSummary> generateFlowUnit() {
    List<HotNodeSummary> unhealthyNodeSummaries = new ArrayList<>();
    long timestamp = clock.millis();
    List<InstanceDetails> clusterNodesDetails = getClusterNodesDetails();
    UsageBucketThresholds usageBucketThresholds = null;
    if (computeUsageBuckets) {
      usageBucketThresholds = getBucketThresholds();
    }
    // iterate through this table
    for (InstanceDetails nodeDetails : clusterNodesDetails) {
      NodeKey nodeKey = new NodeKey(nodeDetails.getInstanceId(), nodeDetails.getInstanceIp());
      // skip if the node is not found in table
      if (!nodeTable.containsRow(nodeKey)) {
        continue;
      }
      HotNodeSummary newNodeSummary = generateNodeSummary(nodeKey);
      if (newNodeSummary != null) {
        if (computeUsageBuckets) {
          for (HotResourceSummary hotResourceSummary : newNodeSummary.getHotResourceSummaryList()) {
            UsageBucket bucket = usageBucketThresholds.computeBucket(hotResourceSummary);
            hotResourceSummary.setUsageBucket(bucket);
          }
        }
        unhealthyNodeSummaries.add(newNodeSummary);
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

  protected UsageBucketThresholds getBucketThresholds() {
    return usageBucketThresholds;
  }

  /**
   * generate summary for node (nodeKey). read the flowunits of all upstream RCAs from
   * this node and generate its node level summary as ouput.
   * The default implementation in this method is to pick the most recent flowunits from the table
   * and check the healthiness of flowunits from all up stream RCAs and whenever any flowunit is
   * unhealthy, we mark the node as unhealthy and append the summary from this flowunit to the nested
   * summary list of this node summary and use this summary as the final output of this method.
   * @param nodeKey NodeKey of the node that we want to generate node summary for
   * @return node summary for this node
   */
  protected HotNodeSummary generateNodeSummary(NodeKey nodeKey) {
    HotNodeSummary nodeSummary = null;
    long timestamp = clock.millis();
    // for each RCA type this cluster RCA subscribes, read its most recent flowunit and if it is
    // unhealthy, append this flowunit to output node summary
    for (Rca<ResourceFlowUnit<HotNodeSummary>> nodeRca : nodeRcas) {
      // skip if we haven't receive any flowunit from this RCA yet.
      if (nodeTable.get(nodeKey, nodeRca.name()) == null) {
        continue;
      }
      ResourceFlowUnit<HotNodeSummary> flowUnit = nodeTable.get(nodeKey, nodeRca.name()).getLast();
      // skip this flowunit if :
      // 1. the timestamp of this flowunit expires
      // 2. flowunit is healthy
      // 3. flowunit does not have summary attached to it
      if (timestamp - flowUnit.getTimeStamp() > TIMESTAMP_EXPIRATION_IN_MILLIS
          || flowUnit.getResourceContext().isHealthy()
          || flowUnit.getSummary() == null) {
        continue;
      }
      if (nodeSummary == null) {
        nodeSummary = new HotNodeSummary(nodeKey.getNodeId(), nodeKey.getHostAddress());
      }
      // append all resource summaries into this
      flowUnit.getSummary().getHotResourceSummaryList().forEach(nodeSummary::appendNestedSummary);
    }
    return nodeSummary;
  }

  @Override
  public ResourceFlowUnit<HotClusterSummary> operate() {
    counter += 1;
    nodeRcas.forEach(this::addUpstreamFlowUnits);

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
