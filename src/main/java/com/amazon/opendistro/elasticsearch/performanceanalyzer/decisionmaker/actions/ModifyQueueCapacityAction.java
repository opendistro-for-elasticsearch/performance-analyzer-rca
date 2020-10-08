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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.DecisionMakerConsts.JSON_PARSER;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.CPU;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.HEAP;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.NETWORK;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.QueueActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
import com.google.gson.JsonObject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ModifyQueueCapacityAction extends SuppressibleAction {

  private static final Logger LOG = LogManager.getLogger(ModifyQueueCapacityAction.class);
  public static final String NAME = "ModifyQueueCapacity";

  private final ResourceEnum threadPool;
  private final NodeKey esNode;
  private final int desiredCapacity;
  private final int currentCapacity;
  private final long coolOffPeriodInMillis;
  private final boolean canUpdate;

  private ModifyQueueCapacityAction(NodeKey esNode, ResourceEnum threadPool, AppContext appContext,
      int desiredCapacity, int currentCapacity, long coolOffPeriodInMillis, boolean canUpdate) {
    super(appContext);
    this.esNode = esNode;
    this.threadPool = threadPool;
    this.desiredCapacity = desiredCapacity;
    this.currentCapacity = currentCapacity;
    this.coolOffPeriodInMillis = coolOffPeriodInMillis;
    this.canUpdate = canUpdate;
  }

  public static Builder newBuilder(NodeKey esNode, ResourceEnum threadPool, final AppContext appContext, final RcaConf conf) {
    return new Builder(esNode, threadPool, appContext, conf);
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
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("Id", esNode.getNodeId().toString());
    jsonObject.addProperty("Ip", esNode.getHostAddress().toString());
    jsonObject.addProperty("resource", threadPool.getNumber());
    jsonObject.addProperty("desiredCapacity", desiredCapacity);
    jsonObject.addProperty("currentCapacity", currentCapacity);
    jsonObject.addProperty("coolOffPeriodInMillis", coolOffPeriodInMillis);
    jsonObject.addProperty("canUpdate", canUpdate);
    return jsonObject.toString();
  }

  // Generates action from summary. Passing in appContext because it contains dynamic settings
  public static ModifyQueueCapacityAction fromSummary(String jsonRepr, AppContext appContext) {
    final JsonObject jsonObject = JSON_PARSER.parse(jsonRepr).getAsJsonObject();

    NodeKey esNode = new NodeKey(new InstanceDetails.Id(jsonObject.get("Id").getAsString()),
            new InstanceDetails.Ip(jsonObject.get("Ip").getAsString()));
    ResourceEnum threadPool = ResourceEnum.forNumber(jsonObject.get("resource").getAsInt());
    int desiredCapacity = jsonObject.get("desiredCapacity").getAsInt();
    int currentCapacity = jsonObject.get("currentCapacity").getAsInt();
    long coolOffPeriodInMillis = jsonObject.get("coolOffPeriodInMillis").getAsLong();
    boolean canUpdate = jsonObject.get("canUpdate").getAsBoolean();

    return new ModifyQueueCapacityAction(esNode, threadPool, appContext,
            desiredCapacity, currentCapacity, coolOffPeriodInMillis, canUpdate);
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
    public static final boolean DEFAULT_IS_INCREASE = true;
    public static final boolean DEFAULT_CAN_UPDATE = true;

    private int stepSize;
    private boolean increase;
    private boolean canUpdate;
    private long coolOffPeriodInMillis;
    private final ResourceEnum threadPool;
    private final NodeKey esNode;
    private final AppContext appContext;
    private final RcaConf rcaConf;

    private Integer currentCapacity;
    private Integer desiredCapacity;
    private final int upperBound;
    private final int lowerBound;

    public Builder(NodeKey esNode, ResourceEnum threadPool, final AppContext appContext, final RcaConf conf) {
      this.esNode = esNode;
      this.threadPool = threadPool;
      this.appContext = appContext;
      this.rcaConf = conf;
      this.coolOffPeriodInMillis = DEFAULT_COOL_OFF_PERIOD_IN_MILLIS;
      this.increase = DEFAULT_IS_INCREASE;
      this.canUpdate = DEFAULT_CAN_UPDATE;
      this.desiredCapacity = null;
      this.currentCapacity = NodeConfigCacheReaderUtil.readQueueCapacity(
          appContext.getNodeConfigCache(), esNode, threadPool);

      QueueActionConfig queueActionConfig = new QueueActionConfig(rcaConf);
      this.upperBound = queueActionConfig.getThresholdConfig(threadPool).upperBound();
      this.lowerBound = queueActionConfig.getThresholdConfig(threadPool).lowerBound();
      this.stepSize = queueActionConfig.getStepSize(threadPool);
    }

    public Builder coolOffPeriod(long coolOffPeriodInMillis) {
      this.coolOffPeriodInMillis = coolOffPeriodInMillis;
      return this;
    }

    public Builder increase(boolean increase) {
      this.increase = increase;
      return this;
    }

    public Builder setDesiredCapacityToMin() {
      this.desiredCapacity = this.lowerBound;
      return this;
    }

    public Builder setDesiredCapacityToMax() {
      this.desiredCapacity = this.upperBound;
      return this;
    }

    public Builder stepSize(int stepSize) {
      this.stepSize = stepSize;
      return this;
    }

    public ModifyQueueCapacityAction build() {
      if (currentCapacity == null) {
        LOG.error("Action: Fail to read queue capacity from node config cache. Return an non-actionable action");
        return new ModifyQueueCapacityAction(esNode, threadPool, appContext,
            -1, -1, coolOffPeriodInMillis, false);
      }
      if (desiredCapacity == null) {
        desiredCapacity = increase ? currentCapacity + stepSize : currentCapacity - stepSize;
      }

      // Ensure desired capacity is within configured safety bounds
      desiredCapacity = Math.min(desiredCapacity, upperBound);
      desiredCapacity = Math.max(desiredCapacity, lowerBound);
      return new ModifyQueueCapacityAction(esNode, threadPool, appContext,
          desiredCapacity, currentCapacity, coolOffPeriodInMillis, canUpdate);
    }
  }
}
