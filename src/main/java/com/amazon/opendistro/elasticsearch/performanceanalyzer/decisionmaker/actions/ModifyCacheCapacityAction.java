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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.HEAP;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModifyCacheCapacityAction implements Action {
  public static final String NAME = "modifyCacheCapacity";
  public static final int COOL_OFF_PERIOD_IN_SECONDS = 300;

  private long currentCapacityInBytes;
  private long desiredCapacityInBytes;
  private ResourceEnum cacheType;
  private NodeKey esNode;

  private Map<ResourceEnum, Integer> upperBound = new HashMap<>();

  public ModifyCacheCapacityAction(
          NodeKey esNode, ResourceEnum cacheType, long currentCapacityInBytes, boolean increase) {
    // TODO: Also consume NodeConfigurationRca
    setBounds();
    // TODO: Update the step size to be a percentage of max size instead of an absolute value
    int STEP_SIZE_IN_BYTES = 50;
    this.esNode = esNode;
    this.cacheType = cacheType;
    this.currentCapacityInBytes = currentCapacityInBytes;
    long desiredCapacity =
            increase ? currentCapacityInBytes + STEP_SIZE_IN_BYTES : currentCapacityInBytes;
    setDesiredCapacity(desiredCapacity);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean isActionable() {
    return desiredCapacityInBytes != currentCapacityInBytes;
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
    final ImpactVector impactVector = new ImpactVector();
    if (desiredCapacityInBytes > currentCapacityInBytes) {
      impactVector.increasesPressure(HEAP);
    } else if (desiredCapacityInBytes < currentCapacityInBytes) {
      impactVector.decreasesPressure(HEAP);
    }
    return Collections.singletonMap(esNode, impactVector);
  }

  @Override
  public void execute() {
    // Making this a no-op for now
    // TODO: Modify based on downstream CoS agent API calls
    assert true;
  }

  @Override
  public String summary() {
    if (!isActionable()) {
      return String.format("No action to take for: [%s]", NAME);
    }
    return String.format("Update [%s] capacity from [%d] to [%d] on node [%s]",
            cacheType.toString(), currentCapacityInBytes, desiredCapacityInBytes, esNode.getNodeId());
  }

  @Override
  public String toString() {
    return summary();
  }

  private void setBounds() {
    // This is intentionally not made static because different nodes can
    // have different bounds based on instance types
    // TODO: Read the upperBound from NodeConfigurationRca.

    // Field data cache used when sorting on or computing aggregation on the field (in Bytes)
    upperBound.put(ResourceEnum.FIELD_DATA_CACHE, 12000);

    // Shard request cache (in Bytes)
    upperBound.put(ResourceEnum.SHARD_REQUEST_CACHE, 12000);
  }

  private void setDesiredCapacity(long desiredCapacity) {
    this.desiredCapacityInBytes = Math.min(desiredCapacity, upperBound.get(cacheType));
  }

  public long getCurrentCapacityInBytes() {
    return currentCapacityInBytes;
  }

  public long getDesiredCapacityInBytes() {
    return desiredCapacityInBytes;
  }

  public ResourceEnum getCacheType() {
    return cacheType;
  }
}
