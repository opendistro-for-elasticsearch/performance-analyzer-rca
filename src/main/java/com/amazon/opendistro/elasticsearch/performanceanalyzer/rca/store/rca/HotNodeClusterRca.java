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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotNodeClusterRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HotNodeClusterRca extends Rca<ResourceFlowUnit<HotClusterSummary>> {

  public static final String RCA_TABLE_NAME = HotNodeClusterRca.class.getSimpleName();
  private static final Logger LOG = LogManager.getLogger(HotNodeClusterRca.class);
  private static final double NODE_COUNT_THRESHOLD = 0.8;
  private static final long TIMESTAMP_EXPIRATION_IN_MINS = 5;
  private final Rca<ResourceFlowUnit<HotNodeSummary>> hotNodeRca;
  private final Table<String, ResourceType, NodeResourceUsage> nodeTable;
  private final int rcaPeriod;
  private int counter;
  private List<NodeDetails> dataNodesDetails;
  private double unbalancedResourceThreshold;
  private double resourceUsageLowerBoundThreshold;
  protected Clock clock;

  public <R extends Rca<ResourceFlowUnit<HotNodeSummary>>> HotNodeClusterRca(final int rcaPeriod,
      final R hotNodeRca) {
    super(5);
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    this.clock = Clock.systemUTC();
    this.hotNodeRca = hotNodeRca;
    nodeTable = HashBasedTable.create();
    unbalancedResourceThreshold = HotNodeClusterRcaConfig.DEFAULT_UNBALANCED_RESOURCE_THRES;
    resourceUsageLowerBoundThreshold = HotNodeClusterRcaConfig.DEFAULT_RESOURCE_USAGE_LOWER_BOUND_THRES;
  }

  //add Resource Summary to the corresponding cell in NodeTable
  private void addSummaryToNodeMap(List<ResourceFlowUnit<HotNodeSummary>> hotNodeRcaFlowUnits) {
    for (ResourceFlowUnit<HotNodeSummary> hotNodeRcaFlowUnit : hotNodeRcaFlowUnits) {
      if (hotNodeRcaFlowUnit.isEmpty()) {
        continue;
      }
      HotNodeSummary nodeSummary = hotNodeRcaFlowUnit.getSummary();
      if (nodeSummary.getNestedSummaryList() == null || nodeSummary.getNestedSummaryList().isEmpty()) {
        continue;
      }
      long timestamp = clock.millis();
      for (GenericSummary summary : nodeSummary.getNestedSummaryList()) {
        if (summary instanceof HotResourceSummary) {
          HotResourceSummary resourceSummary = (HotResourceSummary) summary;
          NodeResourceUsage oldUsage = nodeTable.get(nodeSummary.getNodeID(), ((HotResourceSummary) summary).getResourceType());
          if (oldUsage == null || oldUsage.timestamp < timestamp) {
            nodeTable.put(nodeSummary.getNodeID(), resourceSummary.getResourceType(),
                new NodeResourceUsage(timestamp, resourceSummary));
          }
        } else {
          LOG.error("RCA : unexpected summary type !");
        }
      }
    }
  }

  /**
   * Check if the cluster has unbalanced node or not
   * a node is defined as hot node or unbalanced node if usage of any resource on this
   * node is 30% more than the medium of this type of resource across cluster
   * @return the ResourceFlowUnit that contains summary for unbalanced node(s)
   <p>
   * the nodeTable is a 2 dimensional table indexed by
   * (nodeId, resourceType) and all it does is taking a snapshot of the
   * most recent resource summary from this nodeId indexed by resourceType
   </p>
   */
  private ResourceFlowUnit<HotClusterSummary> checkUnbalancedNode() {
    // NodeID -> HotNodeSummary, store the HotNodeSummary that is generated for each node
    Map<String, HotNodeSummary> nodeSummaryMap = new HashMap<>();

    long currTimestamp = clock.millis();
    //For each resource type, scan over all the nodes in cluster and calculate its medium.
    final List<ResourceType> resourceTypeColumnKeys = ImmutableList.copyOf(nodeTable.columnKeySet());
    for (ResourceType resourceType : resourceTypeColumnKeys) {
      List<NodeResourceUsage> resourceUsages = new ArrayList<>();
      for (NodeDetails nodeDetail : dataNodesDetails) {
        NodeResourceUsage currentUsage = nodeTable.get(nodeDetail.getId(), resourceType);
        // some node does not has this resource type in table.
        if (currentUsage == null) {
          continue;
        }
        // drop the value if the timestamp expires
        if (currTimestamp - currentUsage.timestamp > TimeUnit.MINUTES.toMillis(TIMESTAMP_EXPIRATION_IN_MINS)) {
          nodeTable.row(nodeDetail.getId()).remove(resourceType);
          continue;
        }
        resourceUsages.add(currentUsage);
      }

      //skip this resource type if we have not yet collected enough summaries from data nodes
      int nodeCntThreshold = (int)((double)dataNodesDetails.size() * NODE_COUNT_THRESHOLD);
      //we need at least 2 nodes
      if (nodeCntThreshold < 2) {
        nodeCntThreshold = 2;
      }
      if (resourceUsages.size() < nodeCntThreshold) {
        continue;
      }

      //sort and get the medium value
      resourceUsages.sort(
          Comparator.comparingDouble((NodeResourceUsage r) -> r.resourceSummary.getValue())
      );
      int mediumIdx = resourceUsages.size() / 2;
      if (resourceUsages.size() % 2 == 0) {
        mediumIdx -= 1;
      }
      double medium = resourceUsages.get(mediumIdx).resourceSummary.getValue();

      //iterate the nodeid list again and check if some node is unbalanced
      for (NodeDetails nodeDetail : dataNodesDetails) {
        NodeResourceUsage currentUsage = nodeTable.get(nodeDetail.getId(), resourceType);
        if (currentUsage == null) {
          continue;
        }
        // if the resource value is a outlier.
        // and we also want to make sure the value we get here is large enough.
        // we might want to filter out noise data if the value < 10% of the threshold of that resource type
        if (currentUsage.resourceSummary.getValue() >= medium * (1 + unbalancedResourceThreshold)
            && currentUsage.resourceSummary.getValue()
                >= currentUsage.resourceSummary.getThreshold() * resourceUsageLowerBoundThreshold) {
          if (!nodeSummaryMap.containsKey(nodeDetail.getId())) {
            nodeSummaryMap.put(nodeDetail.getId(),
                new HotNodeSummary(nodeDetail.getId(), nodeDetail.getHostAddress()));
          }
          nodeSummaryMap.get(nodeDetail.getId()).addNestedSummaryList(currentUsage.resourceSummary);
        }
      }
    }

    HotClusterSummary summary = null;
    ResourceContext context = null;
    //create summary for unbalanced nodes
    if (nodeSummaryMap.isEmpty()) {
      context = new ResourceContext(Resources.State.HEALTHY);
    } else {
      context = new ResourceContext(Resources.State.UNHEALTHY);
      summary = new HotClusterSummary(dataNodesDetails.size(), nodeSummaryMap.size());
      for (Map.Entry<String, HotNodeSummary> entry : nodeSummaryMap.entrySet()) {
        summary.addNestedSummaryList(entry.getValue());
      }
    }
    return new ResourceFlowUnit<>(System.currentTimeMillis(), context, summary, true);
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

  @Override
  public ResourceFlowUnit<HotClusterSummary> operate() {
    dataNodesDetails = ClusterDetailsEventProcessor.getDataNodesDetails();
    //skip this RCA if the cluster has only single data node
    if (dataNodesDetails.size() <= 1) {
      return new ResourceFlowUnit<>(System.currentTimeMillis());
    }

    counter += 1;
    addSummaryToNodeMap(hotNodeRca.getFlowUnits());

    if (counter >= rcaPeriod) {
      counter = 0;
      removeInactiveNodeFromTable();
      return checkUnbalancedNode();
    } else {
      return new ResourceFlowUnit<>(System.currentTimeMillis());
    }
  }

  /**
   * read thresholds from rca.conf
   * @param conf RcaConf object
   */
  @Override
  public void readRcaConf(RcaConf conf) {
    HotNodeClusterRcaConfig configObj = conf.getHotNodeClusterRcaConfig();
    unbalancedResourceThreshold = configObj.getUnbalancedResourceThreshold();
    resourceUsageLowerBoundThreshold = configObj.getResourceUsageLowerBoundThreshold();
  }

  /**
   * This is a cluster level RCA vertex which by definition can not be serialize/de-serialized
   * over gRPC.
   */
  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
        + "be required.");
  }

  /**
   * a wrapper to add local timestamp to HotResourceSummary
   */
  private static class NodeResourceUsage {
    private final long timestamp;
    private final HotResourceSummary resourceSummary;

    NodeResourceUsage(final long timestamp, final HotResourceSummary resourceSummary) {
      this.timestamp = timestamp;
      this.resourceSummary = resourceSummary;
    }
  }
}
