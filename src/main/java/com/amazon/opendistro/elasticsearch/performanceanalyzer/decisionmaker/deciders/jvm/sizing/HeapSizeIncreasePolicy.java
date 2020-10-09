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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.sizing;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.HeapSizeIncreaseAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.AlarmMonitor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.DecisionPolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.JvmActionsAlarmMonitor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HeapSizeIncreasePolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.LargeHeapClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.underutilization.ClusterUnderUtilizedRca;
import com.google.common.annotations.VisibleForTesting;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

public class HeapSizeIncreasePolicy implements DecisionPolicy {

  private final LargeHeapClusterRca largeHeapClusterRca;
  private final ClusterUnderUtilizedRca clusterUnderUtilizedRca;
  private final HeapSizeIncreaseClusterMonitor heapSizeIncreaseContentionClusterMonitor;
  private final HeapSizeIncreaseClusterMonitor heapSizeIncreaseUnderUtilizationClusterMonitor;

  private AppContext appContext;
  private RcaConf rcaConf;
  private int unhealthyNodePercentage;

  public HeapSizeIncreasePolicy(final LargeHeapClusterRca largeHeapClusterRca, final
      ClusterUnderUtilizedRca clusterUnderUtilizedRca) {
    this.heapSizeIncreaseContentionClusterMonitor = new HeapSizeIncreaseClusterMonitor();
    this.heapSizeIncreaseUnderUtilizationClusterMonitor = new HeapSizeIncreaseClusterMonitor();
    this.largeHeapClusterRca = largeHeapClusterRca;
    this.clusterUnderUtilizedRca = clusterUnderUtilizedRca;
  }

  @Override
  public List<Action> evaluate() {
    addToClusterMonitor();

    List<Action> actions = new ArrayList<>();
    if (!heapSizeIncreaseContentionClusterMonitor.isHealthy()) {
      getHeapSizeIncreaseActionIfActionable().ifPresent(actions::add);
    }

    // Since both contention and under utilization add the same action, we don't want to add
    // the same action twice. If the action is already added as part of contention, then skip
    // checking for under utilization.
    if (actions.isEmpty()) {
      if (!heapSizeIncreaseUnderUtilizationClusterMonitor.isHealthy()) {
        getHeapSizeIncreaseActionIfActionable().ifPresent(actions::add);
      }
    }

    return actions;
  }

  private Optional<Action> getHeapSizeIncreaseActionIfActionable() {
    final Action heapSizeIncreaseAction = new HeapSizeIncreaseAction(appContext);
    if (heapSizeIncreaseAction.isActionable()) {
      return Optional.of(heapSizeIncreaseAction);
    }

    return Optional.empty();
  }

  private void addToClusterMonitor() {
    addToContentionClusterMonitor();
    addToUnderUtilizationClusterMonitor();
  }

  private void addToContentionClusterMonitor() {
    long currTime = System.currentTimeMillis();
    if (largeHeapClusterRca.getFlowUnits().isEmpty()) {
      return;
    }
    ResourceFlowUnit<HotClusterSummary> flowUnit = largeHeapClusterRca.getFlowUnits().get(0);

    if (flowUnit.getSummary() == null) {
      return;
    }
    List<HotNodeSummary> hotNodeSummaries = flowUnit.getSummary().getHotNodeSummaryList();
    hotNodeSummaries.forEach(hotNodeSummary -> {
      NodeKey nodeKey = new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
      heapSizeIncreaseContentionClusterMonitor.recordIssue(nodeKey, currTime);
    });
  }

  private void addToUnderUtilizationClusterMonitor() {
    long currTime = System.currentTimeMillis();
    if (clusterUnderUtilizedRca.getFlowUnits().isEmpty()) {
      return;
    }

    final ResourceFlowUnit<HotClusterSummary> flowUnit =
        clusterUnderUtilizedRca.getFlowUnits().get(0);

    if (!flowUnit.getResourceContext().isUnderUtilized()) {
      return;
    }

    List<HotNodeSummary> hotNodeSummaries = flowUnit.getSummary().getHotNodeSummaryList();
    hotNodeSummaries.forEach(hotNodeSummary -> {
      NodeKey key = new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
      heapSizeIncreaseUnderUtilizationClusterMonitor.recordIssue(key, currTime);
    });
  }

  private class HeapSizeIncreaseClusterMonitor {

    private static final int DEFAULT_DAY_BREACH_THRESHOLD = 8;
    private static final int DEFAULT_WEEK_BREACH_THRESHOLD = 3;
    private static final String PERSISTENCE_PREFIX = "heap-size-increase-alarm-";
    private final Map<NodeKey, AlarmMonitor> perNodeMonitor;
    private int dayBreachThreshold = DEFAULT_DAY_BREACH_THRESHOLD;
    private int weekBreachThreshold = DEFAULT_WEEK_BREACH_THRESHOLD;

    HeapSizeIncreaseClusterMonitor() {
      this.perNodeMonitor = new HashMap<>();
    }

    public void recordIssue(final NodeKey nodeKey, long currTimeStamp) {
      perNodeMonitor.computeIfAbsent(nodeKey,
          key -> new JvmActionsAlarmMonitor(dayBreachThreshold,
              weekBreachThreshold, Paths.get(RcaConsts.CONFIG_DIR_PATH,
              PERSISTENCE_PREFIX + key.getNodeId().toString())))
                    .recordIssue(currTimeStamp, 1d);
    }

    public boolean isHealthy() {
      int numDataNodesInCluster = appContext.getDataNodeInstances().size();
      double unhealthyCount = 0;
      for (final AlarmMonitor monitor : perNodeMonitor.values()) {
        if (!monitor.isHealthy()) {
          unhealthyCount++;
        }
      }
      return (unhealthyCount / numDataNodesInCluster) * 100d < unhealthyNodePercentage;
    }

    public void setDayBreachThreshold(int dayBreachThreshold) {
      this.dayBreachThreshold = dayBreachThreshold;
    }

    public void setWeekBreachThreshold(int weekBreachThreshold) {
      this.weekBreachThreshold = weekBreachThreshold;
    }
  }

  public void setAppContext(@Nonnull final AppContext appContext) {
    this.appContext = appContext;
  }

  public void setRcaConf(final RcaConf rcaConf) {
    this.rcaConf = rcaConf;
    readThresholdValuesFromConf();
  }

  private void readThresholdValuesFromConf() {
    HeapSizeIncreasePolicyConfig policyConfig = rcaConf.getJvmScaleUpPolicyConfig();
    this.unhealthyNodePercentage = policyConfig.getUnhealthyNodePercentage();
    this.heapSizeIncreaseContentionClusterMonitor.setDayBreachThreshold(policyConfig.getDayBreachThresholdForContention());
    this.heapSizeIncreaseContentionClusterMonitor
        .setWeekBreachThreshold(policyConfig.getWeekBreachThresholdForContention());

    this.heapSizeIncreaseUnderUtilizationClusterMonitor
        .setDayBreachThreshold(policyConfig.getDayBreachThresholdForUnderUtilization());
    this.heapSizeIncreaseUnderUtilizationClusterMonitor
        .setWeekBreachThreshold(policyConfig.getWeekBreachThresholdForUnderUtilization());
  }

  @VisibleForTesting
  public int getUnhealthyNodePercentage() {
    return unhealthyNodePercentage;
  }
}
