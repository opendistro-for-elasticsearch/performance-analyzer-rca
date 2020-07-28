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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import java.util.List;

public class DataMsg {
  String sourceGraphNode;
  List<String> destinationGraphNodes;
  List<? extends GenericFlowUnit> flowUnits;

  public DataMsg(
          String sourceGraphNode, List<String> destinationNodes, List<? extends GenericFlowUnit> flowUnits) {
    this.sourceGraphNode = sourceGraphNode;
    this.destinationGraphNodes = destinationNodes;
    this.flowUnits = flowUnits;
  }

  public String getSourceGraphNode() {
    return sourceGraphNode;
  }

  public List<String> getDestinationGraphNodes() {
    return destinationGraphNodes;
  }

  public List<? extends GenericFlowUnit> getFlowUnits() {
    return flowUnits;
  }

  @Override
  public String toString() {
    return String.format("Data::from: '%s', to: %s", sourceGraphNode, destinationGraphNodes);
  }
}
