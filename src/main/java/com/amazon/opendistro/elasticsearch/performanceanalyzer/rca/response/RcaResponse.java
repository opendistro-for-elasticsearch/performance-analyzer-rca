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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * RcaResponse contains cluster level info such as cluster state, number of healthy and unhealthy
 * nodes for a particular rca.
 */
public class RcaResponse {
  private String name;
  private String state;
  private Integer numOfNodes;
  private Integer numOfUnhealthyNodes;
  private String timeStamp;
  private List<NodeSummaryResponse> summary;

  public RcaResponse(String name, String state, String timeStamp) {
    this.name = name;
    this.state = state;
    this.timeStamp = timeStamp;
    this.summary = new ArrayList<>();
  }

  public RcaResponse(
      String name,
      String state,
      Integer numOfNodes,
      Integer numOfUnhealthyNodes,
      String timeStamp) {
    this.name = name;
    this.state = state;
    this.numOfNodes = numOfNodes;
    this.numOfUnhealthyNodes = numOfUnhealthyNodes;
    this.timeStamp = timeStamp;
    this.summary = new ArrayList<>();
  }

  public String getName() {
    return name;
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

  public void addSummary(NodeSummaryResponse nodeSummaryResponse) {
    this.summary.add(nodeSummaryResponse);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RcaResponse that = (RcaResponse) o;
    return Objects.equals(name, that.name)
        && Objects.equals(state, that.state)
        && Objects.equals(numOfNodes, that.numOfNodes)
        && Objects.equals(numOfUnhealthyNodes, that.numOfUnhealthyNodes)
        && Objects.equals(timeStamp, that.timeStamp)
        && Objects.equals(new HashSet<>(summary), new HashSet<>(that.summary));
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(name)
        .append(state)
        .append(numOfNodes)
        .append(numOfUnhealthyNodes)
        .append(timeStamp)
        .append(summary)
        .toHashCode();
  }
}
