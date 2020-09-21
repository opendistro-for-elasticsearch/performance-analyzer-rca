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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyJvmGenerationAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.PersistableSlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.util.NodeConfigCacheReaderUtil;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JvmGenerationTuningPolicy computes generation tuning actions which should be taken based on
 * observed issues in an Elasticsearch cluster.
 */
public class JvmGenerationTuningPolicy {
  private static final List<Resource> YOUNG_GEN_UNDERSIZED_SIGNALS = Lists.newArrayList(
      YOUNG_GEN_PROMOTION_RATE,
      FULL_GC_PAUSE_TIME
  );
  private static final List<Resource> YOUNG_GEN_OVERSIZED_SIGNALS = Lists.newArrayList(
      MINOR_GC_PAUSE_TIME,
      OLD_GEN_HEAP_USAGE
  );
  private static final long COOLOFF_PERIOD_IN_MILLIS = 48L * 24L * 60L * 60L * 1000L;

  private final AppContext appContext;

  // Tracks issues which suggest that the young generation is too small
  private PersistableSlidingWindow tooSmallIssues;
  // Tracks issues which suggest that the young generation is too large
  private SlidingWindow<SlidingWindowData> tooLargeIssues;
  private int issueCountThreshold;


  public JvmGenerationTuningPolicy(AppContext appContext) {
    this.appContext = appContext;
    this.issueCountThreshold = (int) Math.ceil(appContext.getDataNodeInstances().size() * .1d);
    this.tooSmallIssues = new PersistableSlidingWindow(48, 1, TimeUnit.HOURS, "/tmp/young_generation_data_rca");
    this.tooLargeIssues = new SlidingWindow<>(1, TimeUnit.HOURS);
  }

  public void record(HotResourceSummary issue) {
    if (YOUNG_GEN_OVERSIZED_SIGNALS.contains(issue.getResource())) {
      tooLargeIssues.next(new SlidingWindowData(Instant.now().toEpochMilli(), 1));
    } else if (YOUNG_GEN_UNDERSIZED_SIGNALS.contains(issue.getResource())) {
      tooSmallIssues.next(new SlidingWindowData(Instant.now().toEpochMilli(), 1));
    }
  }

  public double getCurrentRatio() {
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
    return (int) Math.floor(currentRatio - 1);
  }

  private int getOversizedThreshold() {
    return issueCountThreshold;
  }

  private int getUndersizedThreshold() {
    return issueCountThreshold;
  }

  public List<Action> actions() {
    List<Action> actions = new ArrayList<>();
    if (tooLargeIssues.size() > getOversizedThreshold()) {
      int newRatio = decreaseYoungGeneration();
      if (newRatio >= 1) {
        actions.add(new ModifyJvmGenerationAction(appContext, decreaseYoungGeneration(), COOLOFF_PERIOD_IN_MILLIS, true));
      }
    } else if (tooSmallIssues.size() > getUndersizedThreshold()) {
      int newRatio = increaseYoungGeneration();
      if (newRatio >= 1) {
        actions.add(new ModifyJvmGenerationAction(appContext, increaseYoungGeneration(), COOLOFF_PERIOD_IN_MILLIS, true));
      }
    }
    return actions;
  }
}
