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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ModifyQueueCapacityAction extends SuppressibleAction {

  private static final Logger LOG = LogManager.getLogger(ModifyQueueCapacityAction.class);
  public static final String NAME = "ModifyQueueCapacity";

  private final ResourceEnum threadPool;
  private final NodeKey esNode;
  private final int desiredCapacity;
  private final int currentCapacity;
  private final int lowerBound;
  private final int upperBound;
  private final long coolOffPeriodInMillis;
  private final boolean canUpdate;

  private ModifyQueueCapacityAction(NodeKey esNode, ResourceEnum threadPool, AppContext appContext,
      int desiredCapacity, int currentCapacity, long coolOffPeriodInMillis,
      int lowerBound, int upperBound, boolean canUpdate) {
    super(appContext);
    this.esNode = esNode;
    this.threadPool = threadPool;
    this.desiredCapacity = desiredCapacity;
    this.currentCapacity = currentCapacity;
    this.coolOffPeriodInMillis = coolOffPeriodInMillis;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.canUpdate = canUpdate;
  }

  public static Builder newBuilder(NodeKey esNode, ResourceEnum threadPool, AppContext appContext) {
    return new Builder(esNode, threadPool, appContext);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean canUpdate() {
    return canUpdate && (desiredCapacity != currentCapacity);
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
    ImpactVector impactVector = new ImpactVector();
    if (desiredCapacity > currentCapacity) {
      impactVector.increasesPressure(HEAP, CPU, NETWORK);
    } else if (desiredCapacity < currentCapacity) {
      impactVector.decreasesPressure(HEAP, CPU, NETWORK);
    }
    return Collections.singletonMap(esNode, impactVector);
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

  public int getCurrentCapacity() {
    return currentCapacity;
  }

  public int getDesiredCapacity() {
    return desiredCapacity;
  }

  public ResourceEnum getThreadPool() {
    return threadPool;
  }

  public static final class Builder {
    public static final long DEFAULT_COOL_OFF_PERIOD_IN_MILLIS = 300 * 1_000;
    public static final int DEFAULT_STEP_SIZE = 50;
    public static final boolean DEFAULT_IS_INCREASE = true;
    public static final boolean DEFAULT_CAN_UPDATE = true;

    private int stepSize;
    private boolean isIncrease;
    private boolean canUpdate;
    private long coolOffPeriodInMillis;
    private int upperBound;
    private int lowerBound;
    private final ResourceEnum threadPool;
    private final NodeKey esNode;
    private final AppContext appContext;

    private Integer currentCapacity;
    private Integer desiredCapacity;

    public Builder(NodeKey esNode, ResourceEnum threadPool, AppContext appContext) {
      this.esNode = esNode;
      this.threadPool = threadPool;
      this.appContext = appContext;
      this.coolOffPeriodInMillis = DEFAULT_COOL_OFF_PERIOD_IN_MILLIS;
      this.stepSize = DEFAULT_STEP_SIZE;
      this.isIncrease = DEFAULT_IS_INCREASE;
      this.canUpdate = DEFAULT_CAN_UPDATE;
      this.desiredCapacity = null;
      this.currentCapacity =
          NodeConfigCacheReaderUtil.readQueueCapacity(appContext.getNodeConfigCache(), esNode, threadPool);
      setDefaultBounds(threadPool);
    }

    private void setDefaultBounds(ResourceEnum threadPool) {
      // TODO: Move configuration values to rca.conf
      switch (threadPool) {
        case WRITE_THREADPOOL:
          this.lowerBound = 100;
          this.upperBound = 1000;
          break;
        case SEARCH_THREADPOOL:
          this.lowerBound = 1000;
          this.upperBound = 3000;
          break;
        default:
          assert false : "unrecognized threadpool type: " + threadPool.name();
      }
    }

    public Builder coolOffPeriod(long coolOffPeriodInMillis) {
      this.coolOffPeriodInMillis = coolOffPeriodInMillis;
      return this;
    }

    public Builder increase(boolean isInrease) {
      this.isIncrease = isInrease;
      return this;
    }

    public Builder desiredCapacity(int desiredCapacity) {
      this.desiredCapacity = desiredCapacity;
      return this;
    }

    public Builder minimalDesiredCapacity() {
      this.desiredCapacity = this.lowerBound;
      return this;
    }

    public Builder stepSize(int stepSize) {
      this.stepSize = stepSize;
      return this;
    }

    public Builder upperBound(int upperBound) {
      this.upperBound = upperBound;
      return this;
    }

    public Builder lowerBound(int lowerBound) {
      this.lowerBound = lowerBound;
      return this;
    }

    public ModifyQueueCapacityAction build() {
      // fail to read capacity from node config cache
      // return an empty non-actionable action object
      if (currentCapacity == null) {
        LOG.error("Action: Fail to read queue capacity from node config cache. Return an non-actionable action");
        return new ModifyQueueCapacityAction(esNode, threadPool, appContext,
            -1, -1, coolOffPeriodInMillis, lowerBound, upperBound, false);
      }
      // skip the step size bound check if we set desiredCapacity
      // explicitly in action builder
      if (desiredCapacity == null) {
        desiredCapacity = isIncrease ? currentCapacity + stepSize : currentCapacity - stepSize;
      }
      desiredCapacity = Math.min(desiredCapacity, upperBound);
      desiredCapacity = Math.max(desiredCapacity, lowerBound);
      return new ModifyQueueCapacityAction(esNode, threadPool, appContext,
          desiredCapacity, currentCapacity, coolOffPeriodInMillis, lowerBound, upperBound, canUpdate);
    }
  }
}
