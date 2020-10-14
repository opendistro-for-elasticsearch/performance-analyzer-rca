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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.JvmGenAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.DecisionPolicy;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.young_gen.JvmGenTuningPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.BucketizedSlidingWindowConfig;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Decides if the JVM heap generations could be resized to improve application
 * performance and suggests actions to take to achieve improved performance.
 */
public class JvmGenTuningPolicy implements DecisionPolicy {
  private static final Logger LOG = LogManager.getLogger(JvmGenTuningPolicy.class);
  private static final long COOLOFF_PERIOD_IN_MILLIS = 2L * 24L * 60L * 60L * 1000L;
  private static final Path UNDERSIZED_DATA_FILE_PATH = Paths.get(RcaConsts.CONFIG_DIR_PATH, "JvmGenerationTuningPolicy_Undersized");
  private static final Path OVERSIZED_DATA_FILE_PATH = Paths.get(RcaConsts.CONFIG_DIR_PATH, "JvmGenerationTuningPolicy_Oversized");
  static final List<Resource> YOUNG_GEN_UNDERSIZED_SIGNALS = Lists.newArrayList(
      YOUNG_GEN_PROMOTION_RATE,
      FULL_GC_PAUSE_TIME
  );
  static final List<Resource> YOUNG_GEN_OVERSIZED_SIGNALS = Lists.newArrayList(
      MINOR_GC_PAUSE_TIME,
      OLD_GEN_HEAP_USAGE
  );

  private AppContext appContext;
  private RcaConf rcaConf;
  private JvmGenTuningPolicyConfig policyConfig;
  private HighHeapUsageClusterRca highHeapUsageClusterRca;

  // Tracks issues which suggest that the young generation is too small
  @VisibleForTesting
  JvmActionsAlarmMonitor tooSmallAlarm;
  // Tracks issues which suggest that the young generation is too large
  @VisibleForTesting
  JvmActionsAlarmMonitor tooLargeAlarm;

  public JvmGenTuningPolicy(HighHeapUsageClusterRca highHeapUsageClusterRca) {
    this(highHeapUsageClusterRca, null, null);
  }

  public JvmGenTuningPolicy(HighHeapUsageClusterRca highHeapUsageClusterRca,
                            JvmActionsAlarmMonitor tooSmallAlarm,
                            JvmActionsAlarmMonitor tooLargeAlarm) {
    this.highHeapUsageClusterRca = highHeapUsageClusterRca;
    this.tooSmallAlarm = tooSmallAlarm;
    this.tooLargeAlarm = tooLargeAlarm;
  }

  /**
   * records issues which the policy cares about and discards others
   * @param issue an issue with the application
   */
  private void record(HotResourceSummary issue) {
    LOG.debug("JVMGenTuningPolicy#record()");
    if (YOUNG_GEN_OVERSIZED_SIGNALS.contains(issue.getResource())) {
      LOG.debug("Recording issue in tooLargeAlarm");
      tooLargeAlarm.recordIssue();
    } else if (YOUNG_GEN_UNDERSIZED_SIGNALS.contains(issue.getResource())) {
      LOG.debug("Recording issue in tooSmallAlarm");
      tooSmallAlarm.recordIssue();
    }
  }

  /**
   * gathers and records all issues observed in the application
   */
  private void recordIssues() {
    if (highHeapUsageClusterRca.getFlowUnits().isEmpty()) {
      return;
    }
    for (ResourceFlowUnit<HotClusterSummary> flowUnit : highHeapUsageClusterRca.getFlowUnits()) {
      if (!flowUnit.hasResourceSummary()) {
        continue;
      }
      HotClusterSummary clusterSummary = flowUnit.getSummary();
      for (HotNodeSummary nodeSummary : clusterSummary.getHotNodeSummaryList()) {
        for (HotResourceSummary summary : nodeSummary.getHotResourceSummaryList()) {
          record(summary);
        }
      }
    }
  }

  /**
   * returns the current old:young generation sizing ratio
   * @return the current old:young generation sizing ratio
   */
  public double getCurrentRatio() {
    LOG.debug("Computing current ratio...");
    if (appContext == null) {
      LOG.debug("JvmGenTuningPolicy AppContext is null");
      return -1;
    }
    NodeConfigCache cache = appContext.getNodeConfigCache();
    NodeKey key = new NodeKey(appContext.getDataNodeInstances().get(0));
    try {
      Double oldGenMaxSizeInBytes = cache.get(key, ResourceUtil.OLD_GEN_MAX_SIZE);
      LOG.debug("old gen max size is {}", oldGenMaxSizeInBytes);
      Double youngGenMaxSizeInBytes = cache.get(key, ResourceUtil.YOUNG_GEN_MAX_SIZE);
      LOG.debug("young gen max size is {}", youngGenMaxSizeInBytes);
      LOG.debug("current ratio is {}", (oldGenMaxSizeInBytes / youngGenMaxSizeInBytes));
      return (oldGenMaxSizeInBytes / youngGenMaxSizeInBytes);
    } catch (IllegalArgumentException | NullPointerException e) {
      LOG.error("Exception while computing old:young generation sizing ratio", e);
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
   * Returns true if the young generation is too small
   * @return true if the young generation is too small
   */
  public boolean youngGenerationIsTooSmall() {
    return !tooSmallAlarm.isHealthy();
  }

  /**
   * Returns true if the young generation is too large
   * @return true if the young generation is too large
   */
  public boolean youngGenerationIsTooLarge() {
    return !tooLargeAlarm.isHealthy();
  }

  public JvmActionsAlarmMonitor createAlarmMonitor(Path persistenceBasePath) {
    BucketizedSlidingWindowConfig dayMonitorConfig = new BucketizedSlidingWindowConfig(
        policyConfig.getDayMonitorWindowSizeMinutes(),
        policyConfig.getDayMonitorBucketSizeMinutes(),
        TimeUnit.MINUTES,
        persistenceBasePath);
    BucketizedSlidingWindowConfig weekMonitorConfig = new BucketizedSlidingWindowConfig(
        policyConfig.getWeekMonitorWindowSizeMinutes(),
        policyConfig.getWeekMonitorBucketSizeMinutes(),
        TimeUnit.MINUTES,
        persistenceBasePath);
    return new JvmActionsAlarmMonitor(policyConfig.getDayBreachThreshold(),
        policyConfig.getWeekBreachThreshold(), UNDERSIZED_DATA_FILE_PATH, dayMonitorConfig, weekMonitorConfig);
  }

  public void initialize() {
    LOG.debug("Initializing alarms...");
    if (tooSmallAlarm == null) {
      tooSmallAlarm = createAlarmMonitor(UNDERSIZED_DATA_FILE_PATH);
    }
    if (tooLargeAlarm == null) {
      tooLargeAlarm = createAlarmMonitor(OVERSIZED_DATA_FILE_PATH);
    }
  }

  @Override
  public List<Action> evaluate() {
    LOG.debug("Evaluating JvmGenTuningPolicy...");
    List<Action> actions = new ArrayList<>();
    if (rcaConf == null || appContext ==  null) {
      LOG.error("rca conf/app context is null, return empty action list");
      return actions;
    }
    policyConfig = rcaConf.getDeciderConfig().getJvmGenTuningPolicyConfig();
    if (!policyConfig.isEnabled()) {
      LOG.debug("JvmGenerationTuningPolicy is disabled");
      return actions;
    }
    initialize();
    LOG.debug("Day breach threshold is {} and week breach threashold is {}",
        tooSmallAlarm.getDayBreachThreshold(), tooSmallAlarm.getWeekBreachThreshold());

    recordIssues();
    if (youngGenerationIsTooLarge()) {
      LOG.debug("The young generation is too large!");
      // only decrease the young generation if the config allows it
      if (policyConfig.allowYoungGenDownsize()) {
        int newRatio = computeDecrease(getCurrentRatio());
        if (newRatio >= 1) {
          actions.add(
              new JvmGenAction(appContext, newRatio, COOLOFF_PERIOD_IN_MILLIS, true));
        }
      }
    } else if (youngGenerationIsTooSmall()) {
      LOG.debug("The young generation is too small!");
      int newRatio = computeIncrease(getCurrentRatio());
      if (newRatio >= 1) {
        LOG.debug("Adding new JvmGenAction with ratio {}", newRatio);
        actions.add(new JvmGenAction(appContext, newRatio, COOLOFF_PERIOD_IN_MILLIS, true));
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
