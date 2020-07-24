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

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The PA agent process is composed of multiple components. The PA Reader and RCA are two such components that are
 * independent in a way they process information but also share some information such as the node and the cluster
 * details. Today, some of these information is accessed by calling static methods and members. This is a bad idea.
 * This class encapsulates such information and is created right at the start in the {@code PerformanceAnalyzerApp}.
 */
public class AppContext {
  private volatile ClusterDetailsEventProcessor clusterDetailsEventProcessor;
  // initiate a node config cache within each AppContext space
  // to store node config settings from ES
  private final NodeConfigCache nodeConfigCache;

  public AppContext() {
    this.clusterDetailsEventProcessor = null;
    this.nodeConfigCache = new NodeConfigCache();
  }

  public AppContext(AppContext other) {
    this.clusterDetailsEventProcessor = new ClusterDetailsEventProcessor(other.clusterDetailsEventProcessor);
  }

  public void setClusterDetailsEventProcessor(final ClusterDetailsEventProcessor clusterDetailsEventProcessor) {
    this.clusterDetailsEventProcessor = clusterDetailsEventProcessor;
  }

  public InstanceDetails getMyInstanceDetails() {
    InstanceDetails ret = new InstanceDetails(AllMetrics.NodeRole.UNKNOWN);

    if (clusterDetailsEventProcessor != null && clusterDetailsEventProcessor.getCurrentNodeDetails() != null) {
      ClusterDetailsEventProcessor.NodeDetails nodeDetails = clusterDetailsEventProcessor.getCurrentNodeDetails();
      ret = new InstanceDetails(
          AllMetrics.NodeRole.valueOf(nodeDetails.getRole()),
          nodeDetails.getId(),
          nodeDetails.getHostAddress(),
          nodeDetails.getIsMasterNode());
    }
    return ret;
  }

  /**
   * Can be used to get all the nodes in the cluster.
   *
   * @return Returns an empty list of the details are not available or else it provides the immutable list of nodes in
   *     the cluster.
   */
  public List<InstanceDetails> getAllClusterInstances() {
    List<InstanceDetails> ret = Collections.EMPTY_LIST;

    if (clusterDetailsEventProcessor != null) {
      ret = getInstanceDetailsFromNodeDetails(clusterDetailsEventProcessor.getNodesDetails());
    }
    return ret;
  }

  public List<InstanceDetails> getDataNodeInstances() {
    List<InstanceDetails> ret = Collections.EMPTY_LIST;
    if (clusterDetailsEventProcessor != null) {
      ret = getInstanceDetailsFromNodeDetails(clusterDetailsEventProcessor.getDataNodesDetails());
    }
    return ret;
  }

  private static List<InstanceDetails> getInstanceDetailsFromNodeDetails(
      final List<ClusterDetailsEventProcessor.NodeDetails> nodeDetails) {
    List<InstanceDetails> instances = new ArrayList<>();

    for (ClusterDetailsEventProcessor.NodeDetails node : nodeDetails) {
      InstanceDetails instanceDetails = new InstanceDetails(
          AllMetrics.NodeRole.valueOf(node.getRole()), node.getId(), node.getHostAddress(), node.getIsMasterNode());
      instances.add(instanceDetails);
    }
    return ImmutableList.copyOf(instances);
  }

  @VisibleForTesting
  public ClusterDetailsEventProcessor getClusterDetailsEventProcessor() {
    return clusterDetailsEventProcessor;
  }

  public List<String> getPeerInstanceIps() {
    return ImmutableList.copyOf(
        getAllClusterInstances()
            .stream()
            .skip(1)  // Skipping the first instance as it is self.
            .map(InstanceDetails::getInstanceIp)
            .collect(Collectors.toList()));
  }

  public NodeConfigCache getNodeConfigCache() {
    return this.nodeConfigCache;
  }
}
