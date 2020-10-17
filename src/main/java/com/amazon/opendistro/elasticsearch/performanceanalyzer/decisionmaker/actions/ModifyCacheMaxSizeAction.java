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
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.HEAP;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.CacheActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    Summary summary = new Summary(
        esNode.getNodeId().toString(),
        esNode.getHostAddress().toString(),
        cacheType.getNumber(),
        desiredCacheMaxSizeInBytes,
        currentCacheMaxSizeInBytes,
        coolOffPeriodInMillis,
        canUpdate);
    return summary.toJson();
  }

  // TODO: we should remove this function from this class and add it as a testing util function instead
  // Generates action from summary. Passing in appContext because it contains dynamic settings
  public static ModifyCacheMaxSizeAction fromSummary(String jsonRepr, AppContext appContext) {
    final JsonObject jsonObject = JSON_PARSER.parse(jsonRepr).getAsJsonObject();
    NodeKey esNode = new NodeKey(new InstanceDetails.Id(jsonObject.get("Id").getAsString()),
            new InstanceDetails.Ip(jsonObject.get("Ip").getAsString()));
    ResourceEnum cacheType = ResourceEnum.forNumber(jsonObject.get("resource").getAsInt());
    long desiredCacheMaxSizeInBytes = jsonObject.get("desiredCacheMaxSizeInBytes").getAsLong();
    long currentCacheMaxSizeInBytes = jsonObject.get("currentCacheMaxSizeInBytes").getAsLong();
    long coolOffPeriodInMillis = jsonObject.get("coolOffPeriodInMillis").getAsLong();
    boolean canUpdate = jsonObject.get("canUpdate").getAsBoolean();

    return new ModifyCacheMaxSizeAction(esNode, cacheType, appContext,
            desiredCacheMaxSizeInBytes, currentCacheMaxSizeInBytes, coolOffPeriodInMillis, canUpdate);
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

  public static long getThresholdInBytes(double threshold, long heapSize) {
    return (long) (threshold * heapSize);
  }

  public static final class Builder {
    public static final long DEFAULT_COOL_OFF_PERIOD_IN_MILLIS = 300 * 1_000;
    public static final boolean DEFAULT_IS_INCREASE = true;
    public static final boolean DEFAULT_CAN_UPDATE = true;

    private final ResourceEnum cacheType;
    private final NodeKey esNode;
    private final AppContext appContext;
    private final RcaConf rcaConf;

    private double stepSizeInPercent;
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

      CacheActionConfig cacheActionConfig = new CacheActionConfig(rcaConf);
      double upperBoundThreshold = cacheActionConfig.getThresholdConfig(cacheType).upperBound();
      double lowerBoundThreshold = cacheActionConfig.getThresholdConfig(cacheType).lowerBound();
      this.stepSizeInPercent = cacheActionConfig.getStepSize(cacheType);
      if (heapMaxSizeInBytes != null) {
        this.upperBoundInBytes = getThresholdInBytes(upperBoundThreshold, heapMaxSizeInBytes);
        this.lowerBoundInBytes = getThresholdInBytes(lowerBoundThreshold, heapMaxSizeInBytes);
      } else {
        // If heapMaxSizeInBytes is null, we return a non-actionable object from build
        this.upperBoundInBytes = 0;
        this.lowerBoundInBytes = 0;
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

    public Builder setDesiredCacheMaxSizeToMin() {
      this.desiredCacheMaxSizeInBytes = lowerBoundInBytes;
      return this;
    }

    public Builder setDesiredCacheMaxSizeToMax() {
      this.desiredCacheMaxSizeInBytes = upperBoundInBytes;
      return this;
    }

    public Builder stepSizeInPercent(double stepSizeInPercent) {
      this.stepSizeInPercent = stepSizeInPercent;
      return this;
    }

    public ModifyCacheMaxSizeAction build() {
      if (currentCacheMaxSizeInBytes == null || heapMaxSizeInBytes == null) {
        LOG.error("Action: Fail to read cache max size or heap max size from node config cache. "
            + "Return an non-actionable action");
        return new ModifyCacheMaxSizeAction(esNode, cacheType, appContext,
            -1, -1, coolOffPeriodInMillis, false);
      }
      // check if fielddata cache is -1 (unbounded).
      if (currentCacheMaxSizeInBytes == -1) {
        currentCacheMaxSizeInBytes = heapMaxSizeInBytes;
      }

      long stepSizeInBytes = (long) (stepSizeInPercent * heapMaxSizeInBytes);
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

  public static class Summary {
    public static final String ID = "Id";
    public static final String IP = "Ip";
    public static final String RESOURCE = "resource";
    public static final String DESIRED_MAX_SIZE = "desiredCacheMaxSizeInBytes";
    public static final String CURRENT_MAX_SIZE = "currentCacheMaxSizeInBytes";
    public static final String COOL_OFF_PERIOD = "coolOffPeriodInMillis";
    public static final String CAN_UPDATE = "canUpdate";
    @SerializedName(value = ID)
    private String id;
    @SerializedName(value = IP)
    private String ip;
    @SerializedName(value = RESOURCE)
    private int resource;
    @SerializedName(value = DESIRED_MAX_SIZE)
    private long desiredCacheMaxSizeInBytes;
    @SerializedName(value = CURRENT_MAX_SIZE)
    private long currentCacheMaxSizeInBytes;
    // TODO: remove coolOffPeriodInMillis and canUpdate from summary
    //  as those already exist in baseline action object
    @SerializedName(value = COOL_OFF_PERIOD)
    private long coolOffPeriodInMillis;
    @SerializedName(value = CAN_UPDATE)
    private boolean canUpdate;

    public Summary(String id, String ip, int resource, long desiredCacheMaxSizeInBytes,
        long currentCacheMaxSizeInBytes, long coolOffPeriodInMillis, boolean canUpdate) {
      this.id = id;
      this.ip = ip;
      this.resource = resource;
      this.desiredCacheMaxSizeInBytes = desiredCacheMaxSizeInBytes;
      this.currentCacheMaxSizeInBytes = currentCacheMaxSizeInBytes;
      this.coolOffPeriodInMillis = coolOffPeriodInMillis;
      this.canUpdate = canUpdate;
    }

    public String toJson() {
      Gson gson = new GsonBuilder().disableHtmlEscaping().create();
      return gson.toJson(this);
    }

    public String getId() {
      return this.id;
    }

    public String getIp() {
      return this.ip;
    }

    public ResourceEnum getResource() {
      return ResourceEnum.forNumber(this.resource);
    }

    public long getCurrentCacheMaxSizeInBytes() {
      return currentCacheMaxSizeInBytes;
    }

    public long getDesiredCacheMaxSizeInBytes() {
      return desiredCacheMaxSizeInBytes;
    }

    public long getCoolOffPeriodInMillis() {
      return coolOffPeriodInMillis;
    }

    public boolean getCanUpdate() {
      return canUpdate;
    }
  }
}
