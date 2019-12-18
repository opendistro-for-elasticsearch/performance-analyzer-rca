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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;


/**
 * Tasklet is a wrapper on top of a node so that a node can be executed based on its dependency
 * order using the Java Executor framework. When the callable returns, just getting the result as
 * flowUnit is not sufficient, we also need to know the node its coming from because the argument to
 * the operate method is a Map of Node.Class and the flow Unit. The TaskletResult is such a pair.
 */
public class TaskletResult {
  private Class<? extends Node> node;
  // private List<GenericFlowUnit> flowUnits;

  public Class<? extends Node> getNode() {
    return node;
  }

  /*
  public List<GenericFlowUnit> getFlowUnits() {
      return flowUnits;
  }


  public TaskletResult(Class<? extends Node> node, List<GenericFlowUnit> flowUnits) {
      this.node = node;
      this.flowUnits = flowUnits;
  }
   */

  public TaskletResult(Class<? extends Node> node) {
    this.node = node;
  }
}
