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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
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

// TODO: Split the cache action into separate actions for different caches.

public class ModifyCacheMaxSizeAction implements Action {
  private static final Logger LOG = LogManager.getLogger(ModifyCacheMaxSizeAction.class);
  public static final String NAME = "modifyCacheCapacity";
  public static final long COOL_OFF_PERIOD_IN_MILLIS = 300 * 1_000;

  private final NodeKey esNode;
  private final ResourceEnum cacheType;
  private final NodeConfigCache nodeConfigCache;
  private final double cacheSizeUpperBound;

  private Long currentCacheMaxSizeInBytes;
  private Long heapMaxSizeInBytes;
  private long cacheUpperBoundInBytes;
  private long desiredCacheMaxSizeInBytes;

  private Map<ResourceEnum, Long> stepSizeInBytes = new HashMap<>();

  private ModifyCacheMaxSizeAction(
      final NodeKey esNode,
      final ResourceEnum cacheType,
      final NodeConfigCache nodeConfigCache,
      final double cacheSizeUpperBound) {

    this.esNode = esNode;
    this.cacheType = cacheType;
    this.nodeConfigCache = nodeConfigCache;
    this.cacheSizeUpperBound = cacheSizeUpperBound;

    setStepSize();
  }

  public ModifyCacheMaxSizeAction(
      final NodeKey esNode,
      final ResourceEnum cacheType,
      final NodeConfigCache nodeConfigCache,
      final double cacheSizeUpperBound,
      final boolean increase) {
    // TODO: Add lower bound for caches
    // TODO: Address cache scaling down  when JVM decider is available

    this(esNode, cacheType, nodeConfigCache, cacheSizeUpperBound);
    if (validateAndSetConfigValues()) {
      long desiredCapacity =
              increase ? currentCacheMaxSizeInBytes + getStepSize(cacheType) :
                         currentCacheMaxSizeInBytes - getStepSize(cacheType);
      setDesiredCacheMaxSize(desiredCapacity);
    }
  }

  public ModifyCacheMaxSizeAction(
      final NodeKey esNode,
      final ResourceEnum cacheType,
      final NodeConfigCache nodeConfigCache,
      final double cacheSizeUpperBound,
      final boolean increase,
      int step) {
    this(esNode, cacheType, nodeConfigCache, cacheSizeUpperBound);
    if (validateAndSetConfigValues()) {
      long desiredCapacity =
          increase ? currentCacheMaxSizeInBytes + step * getStepSize(cacheType) :
                     currentCacheMaxSizeInBytes - step * getStepSize(cacheType);
      setDesiredCacheMaxSize(desiredCapacity);
    }
  }

  public static ModifyCacheMaxSizeAction newMinimalCapacityAction(
      final NodeKey esNode,
      final ResourceEnum cacheType,
      final NodeConfigCache nodeConfigCache,
      final double cacheSizeUpperBound) {
    ModifyCacheMaxSizeAction action = new ModifyCacheMaxSizeAction(esNode, cacheType, nodeConfigCache, cacheSizeUpperBound);
    if (validateAndSetConfigValues()) {
      //TODO : set lower bound to 0 for now
      action.setDesiredCacheMaxSize(0);
    }
    return action;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean isActionable() {
    if (currentCacheMaxSizeInBytes == null) {
      return false;
    }
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
    if (currentCacheMaxSizeInBytes != null) {
      if (desiredCacheMaxSizeInBytes > currentCacheMaxSizeInBytes) {
        impactVector.increasesPressure(HEAP);
      } else if (desiredCacheMaxSizeInBytes < currentCacheMaxSizeInBytes) {
        impactVector.decreasesPressure(HEAP);
      }
    }
    return Collections.singletonMap(esNode, impactVector);
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

  public void setStepSizeForCache(final long stepSizeInBytes) {
    this.stepSizeInBytes.put(cacheType, stepSizeInBytes);
  }

  public Long getCurrentCacheMaxSizeInBytes() {
    return currentCacheMaxSizeInBytes;
  }

  public Long getDesiredCacheMaxSizeInBytes() {
    return desiredCacheMaxSizeInBytes;
  }

  public double getDesiredCapacityInPercent() {
    return (double)desiredCacheMaxSizeInBytes / (double)heapMaxSizeInBytes;
  }

  public ResourceEnum getCacheType() {
    return cacheType;
  }

  private boolean validateAndSetConfigValues() {
    // This is intentionally not made static because different nodes can
    // have different bounds based on instance types

    final Long cacheMaxSize = NodeConfigCacheReaderUtil.readCacheMaxSizeInBytes(nodeConfigCache, esNode, cacheType);
    final Long heapMaxSize = NodeConfigCacheReaderUtil.readHeapMaxSizeInBytes(nodeConfigCache, esNode);

    if (cacheMaxSize == null || cacheMaxSize == 0 || heapMaxSize == null || heapMaxSize == 0) {
      return false;
    } else {
      currentCacheMaxSizeInBytes = cacheMaxSize;
      heapMaxSizeInBytes = heapMaxSize;
      cacheUpperBoundInBytes = (long) (heapMaxSizeInBytes * cacheSizeUpperBound);
      return true;
    }
  }

  private void setStepSize() {
    // TODO: Update the step size to also include percentage of heap size along with absolute value
    // Field data cache having step size of 512MB
    stepSizeInBytes.put(ResourceEnum.FIELD_DATA_CACHE, 512 * 1_024L * 1_024L);

    // Shard request cache step size of 512KB
    stepSizeInBytes.put(ResourceEnum.SHARD_REQUEST_CACHE, 512 * 1_024L);
  }

  private long getStepSize(final ResourceEnum cacheType) {
    return stepSizeInBytes.get(cacheType);
  }

  private void setDesiredCacheMaxSize(final long desiredCacheMaxSize) {
    this.desiredCacheMaxSizeInBytes = Math.min(desiredCacheMaxSize, cacheUpperBoundInBytes);
  }
}
