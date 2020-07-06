/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.CPU;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.HEAP;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.NETWORK;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModifyQueueCapacityAction implements Action {

  public static final String NAME = "modify_queue_capacity";
  public static final int COOL_OFF_PERIOD_IN_SECONDS = 300;

  private int currentCapacity;
  private int desiredCapacity;
  private ResourceEnum threadPool;
  private NodeKey esNode;

  private Map<ResourceEnum, Integer> lowerBound = new HashMap<>();
  private Map<ResourceEnum, Integer> upperBound = new HashMap<>();

  public ModifyQueueCapacityAction(NodeKey esNode, ResourceEnum threadPool, int currentCapacity, boolean increase) {
    setBounds();
    int STEP_SIZE = 50;
    this.esNode = esNode;
    this.threadPool = threadPool;
    this.currentCapacity = currentCapacity;
    int desiredCapacity = increase ? currentCapacity + STEP_SIZE : currentCapacity - STEP_SIZE;
    setDesiredCapacity(desiredCapacity);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean isActionable() {
    return desiredCapacity != currentCapacity;
  }

  @Override
  public int coolOffPeriodInSeconds() {
    return COOL_OFF_PERIOD_IN_SECONDS;
  }

  @Override
  public List<NodeKey> impactedNodes() {
    return Collections.singletonList(esNode);
  }

  @Override
  public Map<NodeKey, ImpactVector> impact() {
    ImpactVector impactVector = new ImpactVector();
    if (desiredCapacity > currentCapacity) {
      impactVector.increasesPressure(HEAP, CPU, NETWORK);
    } else if (desiredCapacity < currentCapacity) {
      impactVector.decreasesPressure(HEAP, CPU, NETWORK);
    }
    return Collections.singletonMap(esNode, impactVector);
  }

  @Override
  public void execute() {
    // Making this a no-op for now
    // TODO: Modify based on downstream agent API calls
    assert true;
  }

  @Override
  public String summary() {
    if (!isActionable()) {
      return String.format("No action to take for: [%s]", NAME);
    }
    return String.format("Update [%s] queue capacity from [%d] to [%d] on node [%s]",
        threadPool.toString(), currentCapacity, desiredCapacity, esNode.getNodeId());
  }

  @Override
  public String toString() {
    return summary();
  }

  private void setBounds() {
    // This is intentionally not made static because different nodes can
    // have different bounds based on instance types
    // TODO: Move configuration values to rca.conf

    // Write thread pool for bulk write requests
    this.lowerBound.put(ResourceEnum.WRITE_THREADPOOL, 100);
    this.upperBound.put(ResourceEnum.WRITE_THREADPOOL, 1000);

    // Search thread pool
    this.lowerBound.put(ResourceEnum.SEARCH_THREADPOOL, 1000);
    this.upperBound.put(ResourceEnum.SEARCH_THREADPOOL, 3000);
  }

  private void setDesiredCapacity(int desiredCapacity) {
    this.desiredCapacity = Math.max(Math.min(desiredCapacity, upperBound.get(threadPool)), lowerBound.get(threadPool));
  }

  public int getCurrentCapacity() {
    return currentCapacity;
  }

  public int getDesiredCapacity() {
    return desiredCapacity;
  }

  public ResourceEnum getThreadPool() {
    return threadPool;
  }
}
