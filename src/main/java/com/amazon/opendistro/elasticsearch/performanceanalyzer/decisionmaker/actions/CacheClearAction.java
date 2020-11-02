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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.SuppressFBWarnings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CacheClearAction extends SuppressibleAction {
  public static final String NAME = "CacheClear";

  private final long coolOffPeriodInMillis;
  private final boolean canUpdate;
  private final List<NodeKey> impactedNodes;

  public CacheClearAction(final AppContext appContext,
      final long coolOffPeriodInMillis,
      final boolean canUpdate) {
    super(appContext);
    this.coolOffPeriodInMillis = coolOffPeriodInMillis;
    this.canUpdate = canUpdate;
    this.impactedNodes = appContext.getDataNodeInstances()
        .stream()
        .map(ins -> new NodeKey(ins.getInstanceId(), ins.getInstanceIp()))
        .collect(Collectors.toList());
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean canUpdate() {
    return canUpdate;
  }

  @Override
  public long coolOffPeriodInMillis() {
    return coolOffPeriodInMillis;
  }

  @Override
  public List<NodeKey> impactedNodes() {
    return impactedNodes;
  }

  @Override
  public Map<NodeKey, ImpactVector> impact() {
    Map<NodeKey, ImpactVector> impactedMap = new HashMap<>();
    impactedNodes.forEach(node -> {
          ImpactVector impactVector = new ImpactVector();
          impactVector.decreasesPressure(HEAP);
          impactedMap.put(node, impactVector);
        }
    );
    return impactedMap;
  }

  @Override
  public String summary() {
    Summary summary = new Summary(impactedNodes,
        coolOffPeriodInMillis, canUpdate);
    return summary.toJson();
  }

  @Override
  public String toString() {
    return summary();
  }

  public static Builder newBuilder(final AppContext appContext) {
    return new Builder(appContext);
  }

  public static final class Builder {

    public static final long DEFAULT_COOL_OFF_PERIOD_IN_MILLIS = TimeUnit.MINUTES.toMillis(30);
    public static final boolean DEFAULT_CAN_UPDATE = true;
    private final AppContext appContext;
    private boolean canUpdate;
    private long coolOffPeriodInMillis;

    private Builder(final AppContext appContext) {
      this.appContext = appContext;
      this.coolOffPeriodInMillis = DEFAULT_COOL_OFF_PERIOD_IN_MILLIS;
      this.canUpdate = DEFAULT_CAN_UPDATE;
    }

    public Builder coolOffPeriod(long coolOffPeriodInMillis) {
      this.coolOffPeriodInMillis = coolOffPeriodInMillis;
      return this;
    }

    public CacheClearAction build() {
      return new CacheClearAction(appContext, coolOffPeriodInMillis, canUpdate);
    }
  }

  public static class Summary {

    public static final String ID = "id";
    public static final String IP = "ip";
    public static final String COOL_OFF_PERIOD = "coolOffPeriodInMillis";
    public static final String CAN_UPDATE = "canUpdate";
    @SerializedName(value = ID)
    private String[] id;
    @SerializedName(value = IP)
    private String[] ip;
    @SerializedName(value = COOL_OFF_PERIOD)
    private long coolOffPeriodInMillis;
    @SerializedName(value = CAN_UPDATE)
    private boolean canUpdate;

    public Summary(final List<NodeKey> impactedNodes,
        long coolOffPeriodInMillis,
        boolean canUpdate) {
      int sz = impactedNodes.size();
      id = new String[sz];
      ip = new String[sz];
      for (int i = 0; i < sz; i++) {
        id[i] = impactedNodes.get(i).getNodeId().toString();
        ip[i] = impactedNodes.get(i).getHostAddress().toString();
      }
      this.coolOffPeriodInMillis = coolOffPeriodInMillis;
      this.canUpdate = canUpdate;
    }

    public String toJson() {
      Gson gson = new GsonBuilder().disableHtmlEscaping().create();
      return gson.toJson(this);
    }

    public boolean getCanUpdate() {
      return this.canUpdate;
    }

    public long getCoolOffPeriodInMillis() {
      return this.coolOffPeriodInMillis;
    }
  }
}
