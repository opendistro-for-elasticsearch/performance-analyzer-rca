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

public class NodeSummaryResponse {
  private String nodeId;
  private String ipAddress;
  private List<ResourceSummaryResponse> resourceList;

  public NodeSummaryResponse(String nodeId, String ipAddress) {
    this.nodeId = nodeId;
    this.ipAddress = ipAddress;
    this.resourceList = new ArrayList<>();
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public List<ResourceSummaryResponse> getResourceList() {
    return resourceList;
  }

  public void addResource(ResourceSummaryResponse resource) {
    this.resourceList.add(resource);
  }

  @Override
  public String toString() {
    return "{"
            + "\"NodeId\" : \"" + nodeId + "\","
            + "\"IpAddress\" : \"" + ipAddress + "\","
            + "\"ResourceContext\" : " + getResourceSummary()
            + '}';
  }

  private String getResourceSummary() {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    if (resourceList != null && !resourceList.isEmpty()) {
      for (ResourceSummaryResponse resourceSummaryResponse : resourceList) {
        builder.append(resourceSummaryResponse.toString()).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    builder.append("]");
    return builder.toString();
  }
}
