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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.DecisionPolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.JvmScaleUpPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.LargeHeapClusterRca;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HeapSizeIncreasePolicy implements DecisionPolicy {

  private final LargeHeapClusterRca largeHeapClusterRca;
  private AppContext appContext;
  private RcaConf rcaConf;
  private PerNodeSlidingWindow perNodeSlidingWindow;
  private long evalFrequency;
  private long counter;
  private int unhealthyNodePercentage;
  private int minimumMinutesUnhealthy;

  private List<Action> prevActionList;

  public HeapSizeIncreasePolicy(final LargeHeapClusterRca largeHeapClusterRca,
      final long policyEvaluationFrequency) {
    this.largeHeapClusterRca = largeHeapClusterRca;
    this.evalFrequency = policyEvaluationFrequency;
    this.counter = 0;
    this.perNodeSlidingWindow = new PerNodeSlidingWindow(4, TimeUnit.DAYS);
    this.prevActionList = new ArrayList<>();
  }

  @Override
  public List<Action> evaluate() {
    counter++;
    addToSlidingWindow();
    if (counter == evalFrequency) {
      counter = 0;
      List<Action> actions = evaluateAndEmit();
      prevActionList.clear();
      prevActionList.addAll(actions);
      return actions;
    }

    return prevActionList;
  }

  private void addToSlidingWindow() {
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
      perNodeSlidingWindow.next(nodeKey, new SlidingWindowData(currTime, 1d));
    });
  }

  private List<Action> evaluateAndEmit() {
    List<Action> actions = new ArrayList<>();
    int numNodesInCluster = appContext.getAllClusterInstances().size();
    int numNodesInClusterUndersizedOldGen = getUnderSizedOldGenCount();

    if ((numNodesInClusterUndersizedOldGen / (double) numNodesInCluster) * 100d >= unhealthyNodePercentage) {
      Action jvmSizeUpAction = new HeapSizeIncreaseAction(appContext);
      if (jvmSizeUpAction.isActionable()) {
        actions.add(jvmSizeUpAction);
      }
    }

    return actions;
  }

  /**
   * Gets the number of nodes that have had a significant number of unhealthy data points in the
   * last 96 hours.
   *
   * @return number of nodes that cross the threshold for unhealthy data points in the last 96
   * hours.
   */
  private int getUnderSizedOldGenCount() {
    int count = 0;
    for (NodeKey key : perNodeSlidingWindow.perNodeSlidingWindow.keySet()) {
      if (perNodeSlidingWindow.readCount(key) >= minimumMinutesUnhealthy) {
        count++;
      }
    }

    return count;
  }

  private static class PerNodeSlidingWindow {
    private final int slidingWindowSize;
    private final TimeUnit windowSizeTimeUnit;
    private final Map<NodeKey, SlidingWindow<SlidingWindowData>> perNodeSlidingWindow;

    public PerNodeSlidingWindow(final int slidingWindowSize, final TimeUnit timeUnit) {
      this.slidingWindowSize = slidingWindowSize;
      this.windowSizeTimeUnit = timeUnit;
      this.perNodeSlidingWindow = new HashMap<>();
    }

    public void next(NodeKey node, SlidingWindowData data) {
      perNodeSlidingWindow.computeIfAbsent(node, n1 -> new SlidingWindow<>(slidingWindowSize,
          windowSizeTimeUnit)).next(data);
    }

    public int readCount(NodeKey node) {
      if (perNodeSlidingWindow.containsKey(node)) {
        SlidingWindow<SlidingWindowData> slidingWindow = perNodeSlidingWindow.get(node);
        double count = slidingWindow.readSum();
        return (int)count;
      }

      return 0;
    }
  }

  public void setAppContext(final AppContext appContext) {
    this.appContext = appContext;
  }

  public void setRcaConf(final RcaConf rcaConf) {
    this.rcaConf = rcaConf;
    readThresholdValuesFromConf();
  }

  private void readThresholdValuesFromConf() {
    JvmScaleUpPolicyConfig policyConfig = rcaConf.getJvmScaleUpPolicyConfig();
    this.unhealthyNodePercentage = policyConfig.getUnhealthyNodePercentage();
    this.minimumMinutesUnhealthy = policyConfig.getMinUnhealthyMinutes();
  }
}
