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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JvmGenAction modifies a generational Garbage Collector's tuning parameters
 *
 * <p>This class is currently used to tune the young generation size when the CMS collector is being used
 */
public class JvmGenAction extends SuppressibleAction {
  public static final String NAME = "JvmGenAction";
  private static final ImpactVector NO_IMPACT = new ImpactVector();

  private final long targetRatio;
  private final long coolOffPeriodInMillis;
  private final boolean canUpdate;

  public JvmGenAction(
      final AppContext appContext,
      final int targetRatio,
      final long coolOffPeriodInMillis,
      final boolean canUpdate) {
    super(appContext);
    this.targetRatio = targetRatio;
    this.coolOffPeriodInMillis = coolOffPeriodInMillis;
    this.canUpdate = canUpdate;
  }

  public long getTargetRatio() {
    return targetRatio;
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
    // all nodes are impacted by this change
    return appContext.getDataNodeInstances().stream().map(NodeKey::new).collect(Collectors.toList());
  }

  /* TODO we can guess at this more accurately from metrics, but increasing/decreasing may have different
      impacts at different times */
  @Override
  public Map<NodeKey, ImpactVector> impact() {
    Map<NodeKey, ImpactVector> impact = new HashMap<>();
    for (NodeKey key : impactedNodes()) {
      impact.put(key, NO_IMPACT);
    }
    return impact;
  }

  @Override
  public String summary() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("resource", ResourceEnum.YOUNG_GEN.getNumber());
    jsonObject.addProperty("targetRatio", targetRatio);
    jsonObject.addProperty("coolOffPeriodInMillis", coolOffPeriodInMillis);
    jsonObject.addProperty("canUpdate", canUpdate);
    return jsonObject.toString();
  }

  @Override
  public String toString() {
    return summary();
  }
}
