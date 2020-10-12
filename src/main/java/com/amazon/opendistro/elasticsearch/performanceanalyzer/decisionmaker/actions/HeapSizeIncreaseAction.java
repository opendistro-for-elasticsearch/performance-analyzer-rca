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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class HeapSizeIncreaseAction extends SuppressibleAction {

  public static final String NAME = "HeapSizeIncreaseAction";
  private static final String SUMMARY = "Update heap size to 128GB";
  private static final String ID_KEY = "Id";
  private static final String IP_KEY = "Ip";
  private static final String CAN_UPDATE_KEY = "canUpdate";
  private final boolean canUpdate;
  private final NodeKey esNode;
  private static final long DEFAULT_COOL_OFF_PERIOD_IN_MILLIS = TimeUnit.DAYS.toMillis(3);
  private static final long GB_TO_B = 1024 * 1024 * 1024;

  public HeapSizeIncreaseAction(@Nonnull final AppContext appContext) {
    super(appContext);
    this.esNode = new NodeKey(appContext.getMyInstanceDetails());
    this.canUpdate = Runtime.getRuntime().totalMemory() > 200;
  }

  /**
   * Constructor used when building the action from a summary.
   */
  public HeapSizeIncreaseAction(final NodeKey nodeKey, final boolean canUpdate,
      final AppContext appContext) {
    super(appContext);
    this.esNode = nodeKey;
    this.canUpdate = canUpdate;
  }

  @Override
  public boolean canUpdate() {
    return canUpdate;
  }

  @Override
  public long coolOffPeriodInMillis() {
    return DEFAULT_COOL_OFF_PERIOD_IN_MILLIS;
  }

  @Override
  public List<NodeKey> impactedNodes() {

    return appContext.getDataNodeInstances()
                     .stream()
                     .map(NodeKey::new).collect(Collectors.toList());
  }

  @Override
  public Map<NodeKey, ImpactVector> impact() {
    final Map<NodeKey, ImpactVector> impactMap = new HashMap<>();
    for (NodeKey nodeKey : impactedNodes()) {
      final ImpactVector impactVector = new ImpactVector();
      impactVector.decreasesPressure(Dimension.HEAP);
      impactMap.put(nodeKey, impactVector);
    }

    return impactMap;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String summary() {
    JsonObject summaryJson = new JsonObject();
    summaryJson.addProperty(ID_KEY, esNode.getNodeId().toString());
    summaryJson.addProperty(IP_KEY, esNode.getHostAddress().toString());
    summaryJson.addProperty(CAN_UPDATE_KEY, canUpdate);

    return summaryJson.toString();
  }

  public static HeapSizeIncreaseAction fromSummary(@Nonnull final String summary,
      @Nonnull final AppContext appContext) {
    JsonObject jsonObject = JsonParser.parseString(summary).getAsJsonObject();
    NodeKey node = new NodeKey(new Id(jsonObject.get(ID_KEY).getAsString()),
        new Ip(jsonObject.get(IP_KEY).getAsString()));
    boolean canUpdateFlag = jsonObject.get(CAN_UPDATE_KEY).getAsBoolean();

    return new HeapSizeIncreaseAction(node, canUpdateFlag,
        appContext);
  }
}
