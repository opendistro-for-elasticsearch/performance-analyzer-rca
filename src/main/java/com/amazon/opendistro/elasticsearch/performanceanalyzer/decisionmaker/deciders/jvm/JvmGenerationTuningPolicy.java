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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.FULL_GC_PAUSE_TIME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.MINOR_GC_PAUSE_TIME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.OLD_GEN_HEAP_USAGE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.YOUNG_GEN_PROMOTION_RATE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.JvmGenerationAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.DecisionPolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.JvmGenerationTuningPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.PersistableSlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * JvmGenerationTuningPolicy decides if the JVM heap generations could be resized to improve application
 * performance and suggests actions to take to achieve improved performance.
 */
public class JvmGenerationTuningPolicy implements DecisionPolicy {
  private static final Logger LOG = LogManager.getLogger(JvmGenerationTuningPolicy.class);
  static final List<Resource> YOUNG_GEN_UNDERSIZED_SIGNALS = Lists.newArrayList(
      YOUNG_GEN_PROMOTION_RATE,
      FULL_GC_PAUSE_TIME
  );
  static final List<Resource> YOUNG_GEN_OVERSIZED_SIGNALS = Lists.newArrayList(
      MINOR_GC_PAUSE_TIME,
      OLD_GEN_HEAP_USAGE
  );
  private static final long COOLOFF_PERIOD_IN_MILLIS = 48L * 24L * 60L * 60L * 1000L;
  private static final Path UNDERSIZED_DATA_FILE_PATH = Paths.get(RcaConsts.RCA_CONF_PATH, "JvmGenerationTuningPolicy_Undersized");
  private static final Path OVERSIZED_DATA_FILE_PATH = Paths.get(RcaConsts.RCA_CONF_PATH, "JvmGenerationTuningPolicy_Oversized");

  private AppContext appContext;
  private RcaConf rcaConf;
  private JvmGenerationTuningPolicyConfig policyConfig;
  private HighHeapUsageClusterRca highHeapUsageClusterRca;

  // Tracks issues which suggest that the young generation is too small
  @VisibleForTesting
  PersistableSlidingWindow tooSmallIssues;
  // Tracks issues which suggest that the young generation is too large
  @VisibleForTesting
  PersistableSlidingWindow tooLargeIssues;

  public JvmGenerationTuningPolicy(HighHeapUsageClusterRca highHeapUsageClusterRca) {
    this.highHeapUsageClusterRca = highHeapUsageClusterRca;
  }

  /**
   * records issues which the policy cares about and discards others
   * @param issue an issue with the application
   */
  private void record(HotResourceSummary issue) {
    if (YOUNG_GEN_OVERSIZED_SIGNALS.contains(issue.getResource())) {
      tooLargeIssues.next(new SlidingWindowData(Instant.now().toEpochMilli(), 1));
    } else if (YOUNG_GEN_UNDERSIZED_SIGNALS.contains(issue.getResource())) {
      tooSmallIssues.next(new SlidingWindowData(Instant.now().toEpochMilli(), 1));
    }
  }

  /**
   * gathers and records all issues observed in the application
   */
  private void recordIssues() {
    if (highHeapUsageClusterRca.getFlowUnits().isEmpty()) {
      return;
    }

    ResourceFlowUnit<HotClusterSummary> flowUnit = highHeapUsageClusterRca.getFlowUnits().get(0);
    if (!flowUnit.hasResourceSummary()) {
      return;
    }

    HotClusterSummary clusterSummary = flowUnit.getSummary();
    for (HotNodeSummary nodeSummary : clusterSummary.getHotNodeSummaryList()) {
      for (HotResourceSummary summary : nodeSummary.getHotResourceSummaryList()) {
        record(summary);
      }
    }
  }

  /**
   * returns the current old:young generation sizing ratio
   * @return the current old:young generation sizing ratio
   */
  public double getCurrentRatio() {
    if (appContext == null) {
      return -1;
    }
    NodeConfigCache cache = appContext.getNodeConfigCache();
    NodeKey key = new NodeKey(appContext.getDataNodeInstances().get(0));
    try {
      Double oldGenMaxSizeInBytes = cache.get(key, ResourceUtil.OLD_GEN_MAX_SIZE);
      Double youngGenMaxSizeInBytes = cache.get(key, ResourceUtil.YOUNG_GEN_MAX_SIZE);
      return (oldGenMaxSizeInBytes / youngGenMaxSizeInBytes);
    } catch (IllegalArgumentException | NullPointerException e) {
      return -1;
    }
  }

  /**
   * Computes a ratio that will decrease the young generation size based on the current ratio
   * @return a ratio that will decrease the young generation size based on the current ratio
   */
  public int computeDecrease(double currentRatio) {
    // Don't decrease the (old:young) ratio beyond 5:1
    if (currentRatio < 0 || currentRatio > 5) {
      return -1;
    }
    return (int) Math.floor(currentRatio + 1);
  }

  /**
   * Computes a ratio that will increase the young generation size based on the current ratio
   * @return a ratio that will increase the young generation size based on the current ratio
   */
  public int computeIncrease(double currentRatio) {
    // Don't increase the (old:young) ratio beyond 3:1
    if (currentRatio < 3) {
      return -1;
    }
    // If the current ratio is egregious (e.g. 50:1) set the ratio to 3:1 immediately
    double newRatio = currentRatio > 5 ? 3 : currentRatio - 1;
    return (int) Math.floor(newRatio);
  }


  /**
   * initializes internal data structures based on a configuration
   *
   * <p>this is done because the config isn't necessarily available at construction time
   *
   * @param policyConfig the configuration to use to initialize the data structures
   */
  private void initialize(JvmGenerationTuningPolicyConfig policyConfig) {
    if (this.tooSmallIssues == null) {
      this.tooSmallIssues = new PersistableSlidingWindow(policyConfig.getSlidingWindowSizeInSeconds(),
          TimeUnit.SECONDS,
          UNDERSIZED_DATA_FILE_PATH);
    }
    if (this.tooLargeIssues == null) {
      this.tooLargeIssues = new PersistableSlidingWindow(policyConfig.getSlidingWindowSizeInSeconds(),
          TimeUnit.SECONDS,
          OVERSIZED_DATA_FILE_PATH);
    }
  }

  public boolean youngGenerationIsTooSmall() {
    return tooSmallIssues.readSum() > policyConfig.getUndersizedbucketHeight();
  }

  /**
   * Returns true if the young generation is too large
   *
   * <p>We consider the young generation to be too large if we've seen related RCAs fire recently
   * Recently in this context means within the length of the last bucket
   *
   * @return true if the young generation is too large
   */
  public boolean youngGenerationIsTooLarge() {
    return tooLargeIssues.size() > policyConfig.getOversizedbucketHeight();
  }

  @Override
  public List<Action> evaluate() {
    List<Action> actions = new ArrayList<>();
    if (rcaConf == null || appContext ==  null) {
      LOG.error("rca conf/app context is null, return empty action list");
      return actions;
    }
    policyConfig = rcaConf.getDeciderConfig().getJvmGenerationTuningPolicyConfig();
    if (!policyConfig.isEnabled()) {
      LOG.debug("JvmGenerationTuningPolicy is disabled");
      return actions;
    }
    initialize(policyConfig);
    recordIssues();
    if (youngGenerationIsTooLarge()) {
      // only decrease the young generation if the config allows it
      if (policyConfig.shouldDecreaseYoungGen()) {
        int newRatio = computeDecrease(getCurrentRatio());
        if (newRatio >= 1) {
          actions.add(
              new JvmGenerationAction(appContext, newRatio, COOLOFF_PERIOD_IN_MILLIS, true));
        }
      }
    } else if (youngGenerationIsTooSmall()) {
      int newRatio = computeIncrease(getCurrentRatio());
      if (newRatio >= 1) {
        actions.add(new JvmGenerationAction(appContext, newRatio, COOLOFF_PERIOD_IN_MILLIS, true));
      }
    }
    return actions;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setRcaConf(final RcaConf rcaConf) {
    this.rcaConf = rcaConf;
  }
}
