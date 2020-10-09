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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public class HeapSizeIncreaseAction extends SuppressibleAction {

  public static final String NAME = "HeapSizeIncreaseAction";
  private static final String SUMMARY = "Update heap size to 128GB";
  private final boolean canUpdate;
  private final NodeKey esNode;
  private static final long DEFAULT_COOL_OFF_PERIOD_IN_MILLIS = TimeUnit.DAYS.toMillis(3);
  private static final long GB_TO_B = 1024 * 1024 * 1024;

  public HeapSizeIncreaseAction(@NonNull final AppContext appContext) {
    super(appContext);
    this.esNode = new NodeKey(appContext.getMyInstanceDetails());
    this.canUpdate = Runtime.getRuntime().totalMemory() > 200 * GB_TO_B;
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
    if (!isActionable()) {
      return String.format("No action to take for: [%s]", NAME);
    }

    return SUMMARY;
  }
}
