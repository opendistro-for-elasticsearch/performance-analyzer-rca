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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;

/**
 * The PA agent process is composed of multiple components. The PA Reader and RCA are two such components that are
 * independent in a way they process information but also share some information such as the node and the cluster
 * details. Today, some of these information is accessed by calling static methods and members. This is a bad idea.
 * This class encapsulates such information and is created right at the start in the {@code PerformanceAnalyzerApp}.
 */
public class AppContext {
  private volatile ClusterDetailsEventProcessor clusterDetailsEventProcessor;

  public AppContext() {
    this.clusterDetailsEventProcessor = null;
  }

  public void setClusterDetailsEventProcessor(final ClusterDetailsEventProcessor clusterDetailsEventProcessor) {
    this.clusterDetailsEventProcessor = clusterDetailsEventProcessor;
  }

  public ClusterDetailsEventProcessor getClusterDetailsEventProcessor() {
    return clusterDetailsEventProcessor;
  }

  public InstanceDetails getMyInstanceDetails() {
    ClusterDetailsEventProcessor.NodeDetails nodeDetails = clusterDetailsEventProcessor.getCurrentNodeDetails();

    InstanceDetails ret;
    if (nodeDetails == null) {
      ret = new InstanceDetails(AllMetrics.NodeRole.UNKNOWN);
    } else {
      ret = new InstanceDetails(
          AllMetrics.NodeRole.valueOf(nodeDetails.getRole()),
          nodeDetails.getId(),
          nodeDetails.getHostAddress(),
          nodeDetails.getIsMasterNode());
    }
    return ret;
  }
}
