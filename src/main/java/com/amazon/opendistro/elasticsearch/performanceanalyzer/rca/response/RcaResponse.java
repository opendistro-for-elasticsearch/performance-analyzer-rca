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

public class RcaResponse {
  private String rcaName;
  private String state;
  private Integer numOfNodes;
  private Integer numOfUnhealthyNodes;
  private String timeStamp;
  private List<NodeSummaryResponse> nodeSummaryResponseList;

  public RcaResponse(String rcaName, String state, String timeStamp) {
    this.rcaName = rcaName;
    this.state = state;
    this.timeStamp = timeStamp;
  }

  public RcaResponse(String rcaName,
                     String state,
                     int numOfNodes,
                     int numOfUnhealthyNodes,
                     String timeStamp) {
    this.rcaName = rcaName;
    this.state = state;
    this.numOfNodes = numOfNodes;
    this.numOfUnhealthyNodes = numOfUnhealthyNodes;
    this.timeStamp = timeStamp;
    this.nodeSummaryResponseList = new ArrayList<>();
  }

  public String getRcaName() {
    return rcaName;
  }

  public String getState() {
    return state;
  }

  public Integer getNumOfNodes() {
    return numOfNodes;
  }

  public Integer getNumOfUnhealthyNodes() {
    return numOfUnhealthyNodes;
  }

  public String getTimeStamp() {
    return timeStamp;
  }

  public void addNodeSummaryResponse(NodeSummaryResponse nodeSummaryResponse) {
    this.nodeSummaryResponseList.add(nodeSummaryResponse);
  }

  @Override
  public String toString() {
    return "{"
            + "\"RcaName\" : \"" + rcaName + "\","
            + "\"State\" : \"" + state + "\","
            + "\"NumOfNodes\" : " + numOfNodes + ","
            + "\"NumOfUnhealthyNodes\" : " + numOfUnhealthyNodes + ","
            + "\"TimeStamp\" : \"" + timeStamp + "\","
            + "\"Data\" : " + getSummaryResponseString()
            + '}';
  }

  private String getSummaryResponseString() {
    StringBuilder builder = new StringBuilder();
    builder.append('[');
    if (nodeSummaryResponseList != null && !nodeSummaryResponseList.isEmpty()) {
      for (NodeSummaryResponse nodeSummaryResponse : nodeSummaryResponseList) {
        builder.append(nodeSummaryResponse.toString()).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
    }
    builder.append("]");
    return builder.toString();
  }
}
