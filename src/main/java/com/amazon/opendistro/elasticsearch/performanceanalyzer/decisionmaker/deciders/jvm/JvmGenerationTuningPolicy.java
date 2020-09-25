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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyJvmGenerationParams;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.JvmGenerationTuningPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.PersistableSlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * JvmGenerationTuningPolicy computes generation tuning actions which should be taken based on
 * observed issues in an Elasticsearch cluster.
 */
public class JvmGenerationTuningPolicy {
  private static final Logger LOG = LogManager.getLogger(JvmGenerationTuningPolicy.class);
  private static final List<Resource> YOUNG_GEN_UNDERSIZED_SIGNALS = Lists.newArrayList(
      YOUNG_GEN_PROMOTION_RATE,
      FULL_GC_PAUSE_TIME
  );
  private static final List<Resource> YOUNG_GEN_OVERSIZED_SIGNALS = Lists.newArrayList(
      MINOR_GC_PAUSE_TIME,
      OLD_GEN_HEAP_USAGE
  );
  private static final long COOLOFF_PERIOD_IN_MILLIS = 48L * 24L * 60L * 60L * 1000L;
  private static final String UNDERSIZED_DATA_FILE_PATH = "/tmp/JvmGenerationTuningPolicy_Undersized";
  private static final String OVERSIZED_DATA_FILE_PATH = "/tmp/JvmGenerationTuningPolicy_Oversized";

  private AppContext appContext;
  private RcaConf rcaConf;
  private JvmGenerationTuningPolicyConfig policyConfig;

  // Tracks issues which suggest that the young generation is too small
  private PersistableSlidingWindow tooSmallIssues;
  // Tracks issues which suggest that the young generation is too large
  private PersistableSlidingWindow tooLargeIssues;

  public void record(HotResourceSummary issue) {
    if (YOUNG_GEN_OVERSIZED_SIGNALS.contains(issue.getResource())) {
      tooLargeIssues.next(new SlidingWindowData(Instant.now().toEpochMilli(), 1));
    } else if (YOUNG_GEN_UNDERSIZED_SIGNALS.contains(issue.getResource())) {
      tooSmallIssues.next(new SlidingWindowData(Instant.now().toEpochMilli(), 1));
    }
  }

  public double getCurrentRatio() {
    if (appContext == null) {
      return -1;
    }
    NodeConfigCache cache = appContext.getNodeConfigCache();
    NodeKey key = new NodeKey(appContext.getDataNodeInstances().get(0));
    Double oldGenMaxSizeInBytes = NodeConfigCacheReaderUtil.readOldGenMaxSizeInBytes(cache, key);
    Double youngGenMaxSizeInBytes = NodeConfigCacheReaderUtil.readYoungGenMaxSizeInBytes(cache, key);
    if (oldGenMaxSizeInBytes == null || youngGenMaxSizeInBytes == null) {
      return -1;
    }
    return (oldGenMaxSizeInBytes / youngGenMaxSizeInBytes);
  }

  public int decreaseYoungGeneration() {
    double currentRatio = getCurrentRatio();
    // Don't decrease the (old:young) ratio beyond 5:1
    if (currentRatio < 0 || currentRatio > 5) {
      return -1;
    }
    return (int) Math.floor(currentRatio + 1);
  }

  public int increaseYoungGeneration() {
    double currentRatio = getCurrentRatio();
    // Don't increase the (old:young) ratio beyond 3:1
    if (currentRatio < 3) {
      return -1;
    }
    double newRatio = currentRatio > 5 ? 3 : currentRatio - 1;
    return (int) Math.floor(newRatio);
  }

  public void initializeSlidingWindows(JvmGenerationTuningPolicyConfig policyConfig) {
    if (this.tooSmallIssues == null) {
      this.tooSmallIssues = new PersistableSlidingWindow(policyConfig.getSlidingWindowSizeInSeconds(),
          policyConfig.getBucketSizeInSeconds(),
          TimeUnit.SECONDS,
          UNDERSIZED_DATA_FILE_PATH);
    }
    if (this.tooLargeIssues == null) {
      this.tooLargeIssues = new PersistableSlidingWindow(policyConfig.getSlidingWindowSizeInSeconds(),
          policyConfig.getBucketSizeInSeconds(),
          TimeUnit.SECONDS,
          OVERSIZED_DATA_FILE_PATH);
    }
  }

  public List<Action> actions() {
    if (rcaConf == null || appContext ==  null) {
      LOG.error("rca conf/app context is null, return empty action list");
      return new ArrayList<>();
    }
    policyConfig = rcaConf.getDeciderConfig().getJvmGenerationTuningPolicyConfig();
    initializeSlidingWindows(policyConfig);
    List<Action> actions = new ArrayList<>();
    if (tooLargeIssues.readCurrentBucket() > policyConfig.getOversizedbucketHeight()) {
      int newRatio = decreaseYoungGeneration();
      if (newRatio >= 1) {
        actions.add(new ModifyJvmGenerationParams(appContext, decreaseYoungGeneration(), COOLOFF_PERIOD_IN_MILLIS, true));
      }
    } else if (tooSmallIssues.readCurrentBucket() > policyConfig.getUndersizedbucketHeight()) {
      int newRatio = increaseYoungGeneration();
      if (newRatio >= 1) {
        actions.add(new ModifyJvmGenerationParams(appContext, increaseYoungGeneration(), COOLOFF_PERIOD_IN_MILLIS, true));
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
