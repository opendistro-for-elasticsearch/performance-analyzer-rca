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
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.CacheUtil.KB_TO_BYTES;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.CacheUtil.MB_TO_BYTES;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.CacheActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Action class is used to modify the cache's max size. It is used by cache decider and other
 * deciders to implement actions like increasing the cache's size. Presently, it acts on field data
 * cache and shard request cache.
 */
public class ModifyCacheMaxSizeAction extends SuppressibleAction {
  private static final Logger LOG = LogManager.getLogger(ModifyCacheMaxSizeAction.class);
  public static final String NAME = "ModifyCacheMaxSize";

  private final NodeKey esNode;
  private final ResourceEnum cacheType;

  private final long desiredCacheMaxSizeInBytes;
  private final long currentCacheMaxSizeInBytes;
  private final long coolOffPeriodInMillis;
  private final boolean canUpdate;

  public ModifyCacheMaxSizeAction(
      final NodeKey esNode,
      final ResourceEnum cacheType,
      final AppContext appContext,
      final long desiredCacheMaxSizeInBytes,
      final long currentCacheMaxSizeInBytes,
      final long coolOffPeriodInMillis,
      final boolean canUpdate) {
    super(appContext);
    this.esNode = esNode;
    this.cacheType = cacheType;
    this.desiredCacheMaxSizeInBytes = desiredCacheMaxSizeInBytes;
    this.currentCacheMaxSizeInBytes = currentCacheMaxSizeInBytes;
    this.coolOffPeriodInMillis = coolOffPeriodInMillis;
    this.canUpdate = canUpdate;
  }

  public static Builder newBuilder(
      final NodeKey esNode,
      final ResourceEnum cacheType,
      final AppContext appContext,
      final RcaConf conf) {
    return new Builder(esNode, cacheType, appContext, conf);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean canUpdate() {
    return canUpdate && (desiredCacheMaxSizeInBytes != currentCacheMaxSizeInBytes);
  }

  @Override
  public long coolOffPeriodInMillis() {
    return coolOffPeriodInMillis;
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

  public long getCurrentCacheMaxSizeInBytes() {
    return currentCacheMaxSizeInBytes;
  }

  public long getDesiredCacheMaxSizeInBytes() {
    return desiredCacheMaxSizeInBytes;
  }

  public ResourceEnum getCacheType() {
    return cacheType;
  }

  public static final class Builder {
    public static final long DEFAULT_COOL_OFF_PERIOD_IN_MILLIS = 300 * 1_000;
    public static final boolean DEFAULT_IS_INCREASE = true;
    public static final boolean DEFAULT_CAN_UPDATE = true;

    private final ResourceEnum cacheType;
    private final NodeKey esNode;
    private final AppContext appContext;
    private final RcaConf rcaConf;

    private long stepSizeInBytes;
    private boolean isIncrease;
    private boolean canUpdate;
    private long coolOffPeriodInMillis;

    private Long currentCacheMaxSizeInBytes;
    private Long desiredCacheMaxSizeInBytes;
    private Long heapMaxSizeInBytes;
    private final long upperBoundInBytes;
    private final long lowerBoundInBytes;

    private Builder(
        final NodeKey esNode,
        final ResourceEnum cacheType,
        final AppContext appContext,
        final RcaConf conf) {
      this.esNode = esNode;
      this.cacheType = cacheType;
      this.appContext = appContext;
      this.rcaConf = conf;

      this.coolOffPeriodInMillis = DEFAULT_COOL_OFF_PERIOD_IN_MILLIS;
      this.isIncrease = DEFAULT_IS_INCREASE;
      this.canUpdate = DEFAULT_CAN_UPDATE;

      this.currentCacheMaxSizeInBytes = NodeConfigCacheReaderUtil.readCacheMaxSizeInBytes(
          appContext.getNodeConfigCache(), esNode, cacheType);
      this.heapMaxSizeInBytes = NodeConfigCacheReaderUtil.readHeapMaxSizeInBytes(
          appContext.getNodeConfigCache(), esNode);
      this.desiredCacheMaxSizeInBytes = null;
      setDefaultStepSize(cacheType);

      CacheActionConfig cacheActionConfig = new CacheActionConfig(rcaConf);
      double upperBoundThreshold = cacheActionConfig.getThresholdConfig(cacheType).upperBound();
      double lowerBoundThreshold = cacheActionConfig.getThresholdConfig(cacheType).lowerBound();
      if (heapMaxSizeInBytes != null) {
        this.upperBoundInBytes = (long) (upperBoundThreshold * heapMaxSizeInBytes);
        this.lowerBoundInBytes = (long) (lowerBoundThreshold * heapMaxSizeInBytes);
      } else {
        // If heapMaxSizeInBytes is null, we return a non-actionable object from build
        this.upperBoundInBytes = 0;
        this.lowerBoundInBytes = 0;
      }
    }

    private void setDefaultStepSize(ResourceEnum cacheType) {
      // TODO: Move configuration values to rca.conf
      // TODO: Update the step size to also include percentage of heap size along with absolute value
      switch (cacheType) {
        case FIELD_DATA_CACHE:
          // Field data cache having step size of 512MB
          this.stepSizeInBytes = (long) 512 * MB_TO_BYTES;
          break;
        case SHARD_REQUEST_CACHE:
          // Shard request cache step size of 512KB
          this.stepSizeInBytes = (long) 512 * KB_TO_BYTES;
          break;
        default:
          throw new IllegalArgumentException(String.format("Unrecognizable cache type: [%s]", cacheType.toString()));
      }
    }

    public Builder coolOffPeriod(long coolOffPeriodInMillis) {
      this.coolOffPeriodInMillis = coolOffPeriodInMillis;
      return this;
    }

    public Builder increase(boolean isIncrease) {
      this.isIncrease = isIncrease;
      return this;
    }

    public Builder desiredCacheMaxSize(long desiredCacheMaxSizeInBytes) {
      this.desiredCacheMaxSizeInBytes = desiredCacheMaxSizeInBytes;
      return this;
    }

    public Builder setDesiredCacheMaxSizeToMin() {
      this.desiredCacheMaxSizeInBytes = lowerBoundInBytes;
      return this;
    }

    public Builder setDesiredCacheMaxSizeToMax() {
      this.desiredCacheMaxSizeInBytes = upperBoundInBytes;
      return this;
    }

    public Builder stepSizeInBytes(long stepSizeInBytes) {
      this.stepSizeInBytes = stepSizeInBytes;
      return this;
    }

    public ModifyCacheMaxSizeAction build() {
      if (currentCacheMaxSizeInBytes == null || heapMaxSizeInBytes == null) {
        LOG.error("Action: Fail to read cache max size or heap max size from node config cache. "
            + "Return an non-actionable action");
        return new ModifyCacheMaxSizeAction(esNode, cacheType, appContext,
            -1, -1, coolOffPeriodInMillis, false);
      }

      if (desiredCacheMaxSizeInBytes == null) {
        desiredCacheMaxSizeInBytes = isIncrease ? currentCacheMaxSizeInBytes + stepSizeInBytes :
            currentCacheMaxSizeInBytes - stepSizeInBytes;
      }

      // Ensure desired cache max size is within thresholds
      desiredCacheMaxSizeInBytes = Math.max(Math.min(desiredCacheMaxSizeInBytes, upperBoundInBytes), lowerBoundInBytes);

      return new ModifyCacheMaxSizeAction(esNode, cacheType, appContext,
          desiredCacheMaxSizeInBytes, currentCacheMaxSizeInBytes, coolOffPeriodInMillis, canUpdate);
    }
  }
}
