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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.CacheDeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Action class is used to modify the cache's max size. It is used by cache decider and other
 * deciders to implement actions like increasing the cache's size. Presently, it acts on field data
 * cache and shard request cache.
 */
public class ModifyCacheMaxSizeAction implements Action {
  private static final Logger LOG = LogManager.getLogger(ModifyCacheMaxSizeAction.class);
  public static final String NAME = "modifyCacheCapacity";
  public static final long COOL_OFF_PERIOD_IN_MILLIS = 300 * 1_000;

  private long currentCacheMaxSizeInBytes;
  private long desiredCacheMaxSizeInBytes;
  private long heapMaxSizeInBytes;

  private double fieldDataCacheSizeUpperBound;
  private double shardRequestCacheSizeUpperBound;

  private ResourceEnum cacheType;
  private NodeKey esNode;

  private Map<ResourceEnum, Long> stepSizeInBytes = new HashMap<>();
  private Map<ResourceEnum, Long> upperBoundInBytes = new HashMap<>();

  public ModifyCacheMaxSizeAction(
      final NodeKey esNode,
      final ResourceEnum cacheType,
      final long currentCacheMaxSizeInBytes,
      final long heapMaxSizeInBytes,
      final double fieldDataCacheSizeUpperBound,
      final double shardRequestCacheSizeUpperBound,
      final boolean increase) {
    // TODO: Add lower bound for caches
    this.fieldDataCacheSizeUpperBound = fieldDataCacheSizeUpperBound;
    this.shardRequestCacheSizeUpperBound = shardRequestCacheSizeUpperBound;
    this.heapMaxSizeInBytes = heapMaxSizeInBytes;

    setBounds();
    setStepSize();

    this.esNode = esNode;
    this.cacheType = cacheType;
    this.currentCacheMaxSizeInBytes = currentCacheMaxSizeInBytes;
    // TODO: Address cache scaling down  when JVM decider is available
    long desiredCapacity =
        increase ? currentCacheMaxSizeInBytes + getStepSize(cacheType) : currentCacheMaxSizeInBytes;
    setDesiredCacheMaxSize(desiredCapacity);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean isActionable() {
    return desiredCacheMaxSizeInBytes != currentCacheMaxSizeInBytes;
  }

  @Override
  public long coolOffPeriodInMillis() {
    return COOL_OFF_PERIOD_IN_MILLIS;
  }

  @Override
  public List<NodeKey> impactedNodes() {
    return Collections.singletonList(esNode);
  }

  @Override
  public Map<NodeKey, ImpactVector> impact() {
    final ImpactVector impactVector = new ImpactVector();
    if (desiredCacheMaxSizeInBytes > currentCacheMaxSizeInBytes) {
      impactVector.increasesPressure(HEAP);
    } else if (desiredCacheMaxSizeInBytes < currentCacheMaxSizeInBytes) {
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
    return String.format(
        "Update [%s] capacity from [%d] to [%d] on node [%s]",
        cacheType.toString(),
        currentCacheMaxSizeInBytes,
        desiredCacheMaxSizeInBytes,
        esNode.getNodeId());
  }

  @Override
  public String toString() {
    return summary();
  }

  private void setBounds() {
    // This is intentionally not made static because different nodes can
    // have different bounds based on instance types

    // Field data cache used when sorting on or computing aggregation on the field (in Bytes)
    upperBoundInBytes.put(
        ResourceEnum.FIELD_DATA_CACHE, (long) (heapMaxSizeInBytes * fieldDataCacheSizeUpperBound));

    // Shard request cache (in Bytes)
    upperBoundInBytes.put(
        ResourceEnum.SHARD_REQUEST_CACHE,
        (long) (heapMaxSizeInBytes * shardRequestCacheSizeUpperBound));
  }

  private void setStepSize() {
    // TODO: Update the step size to also include percentage of heap size along with absolute value
    // Field data cache having step size of 512MB
    stepSizeInBytes.put(ResourceEnum.FIELD_DATA_CACHE, 512 * 1_000_000L);

    // Shard request cache step size of 512KB
    stepSizeInBytes.put(ResourceEnum.SHARD_REQUEST_CACHE, 512 * 1_000L);
  }

  private long getStepSize(final ResourceEnum cacheType) {
    return stepSizeInBytes.get(cacheType);
  }

  private void setDesiredCacheMaxSize(final long desiredCacheMaxSize) {
    this.desiredCacheMaxSizeInBytes =
        Math.min(desiredCacheMaxSize, upperBoundInBytes.get(cacheType));
  }

  public long getCurrentCacheMaxSizeInBytes() {
    return currentCacheMaxSizeInBytes;
  }

  public long getDesiredCacheMaxSizeInBytes() {
    return desiredCacheMaxSizeInBytes;
  }

  public ResourceEnum getCacheType() {
    return cacheType;
  }
}
