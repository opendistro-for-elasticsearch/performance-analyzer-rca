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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

public abstract class LeafNode<T extends GenericFlowUnit> extends Node<T> implements Gatherable<T> {
  private boolean addedToFlowField;

  public LeafNode(int level, long evaluationIntervalSeconds) {
    super(level, evaluationIntervalSeconds);
    Stats stats = Stats.getInstance();
    stats.incrementLeafNodesCount();
  }

  public boolean isAddedToFlowField() {
    return addedToFlowField;
  }

  public void setAddedToFlowField() {
    this.addedToFlowField = true;
  }
}
