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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * NodeSummaryResponse contains summary of different resources of data nodes and also contains info like  NodeId
 * and ip address of node.
 */
public class NodeSummaryResponse {
  private String nodeId;
  private String ipAddress;
  private List<ResourceSummaryResponse> resourceContext;

  public NodeSummaryResponse(String nodeId, String ipAddress) {
    this.nodeId = nodeId;
    this.ipAddress = ipAddress;
    this.resourceContext = new ArrayList<>();
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public List<ResourceSummaryResponse> getResourceList() {
    return resourceContext;
  }

  public void addResource(ResourceSummaryResponse resource) {
    this.resourceContext.add(resource);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeSummaryResponse that = (NodeSummaryResponse) o;
    return Objects.equals(nodeId, that.nodeId)
            && Objects.equals(ipAddress, that.ipAddress)
            && Objects.equals(resourceContext, that.resourceContext);
  }
}
