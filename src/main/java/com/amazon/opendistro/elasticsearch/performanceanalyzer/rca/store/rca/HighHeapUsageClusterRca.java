/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.JvmUsageBucketThresholds;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.StaticBucketThresholds;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.UsageBucketThresholds;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaVerticesMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;

/**
 * This RCA runs on the elected master only and it subscribes all high heap RCA from data nodes
 * within the entire cluster. This can help to reduce the network bandwidth/workload on master node
 * and push computation related workload on data node itself. The RCA uses a cache to keep track of
 * the last three metrics from each node and will mark the node as unhealthy if the last three
 * consecutive flowunits are unhealthy. And if any node is unthleath, the entire cluster will be
 * considered as unhealthy and send out corresponding flowunits to downstream nodes.
 */
public class HighHeapUsageClusterRca extends BaseClusterRca {
  private static final int UNHEALTHY_FLOWUNIT_THRESHOLD = 3;
  private List<Double> youngGenHeapPromotionRateThresholds = Lists.newArrayList(20.0, 40.0, 75.0);
  private List<Double> oldGenHeapUsageThresholds = Lists.newArrayList(20.0, 40.0, 75.0);

  public static final String RCA_TABLE_NAME = HighHeapUsageClusterRca.class.getSimpleName();

  @Override
  public void readRcaConf(RcaConf conf) {
    youngGenHeapPromotionRateThresholds = conf.getUsageBucketThresholds(
        UsageBucketThresholds.YOUNG_GEN_PROMOTION_RATE);
    oldGenHeapUsageThresholds = conf.getUsageBucketThresholds(
        UsageBucketThresholds.OLD_GEN_HEAP_USAGE);
    super.readRcaConf(conf);
  }

  public <R extends Rca<ResourceFlowUnit<HotNodeSummary>>> HighHeapUsageClusterRca(
      final int rcaPeriod,
      final R hotNodeRca) {
    super(rcaPeriod, hotNodeRca);
    this.numOfFlowUnitsInMap = UNHEALTHY_FLOWUNIT_THRESHOLD;
    this.setCollectFromMasterNode(false);
    this.computeUsageBuckets = true;
  }

  @Override
  protected UsageBucketThresholds getBucketThresholds() {
    StaticBucketThresholds youngGenHeapPromotionRateThresholds =
        new StaticBucketThresholds(this.youngGenHeapPromotionRateThresholds.get(0),
            this.youngGenHeapPromotionRateThresholds.get(1),
            this.youngGenHeapPromotionRateThresholds.get(2));
    StaticBucketThresholds oldGenHeapUsageThresholds =
        new StaticBucketThresholds(this.oldGenHeapUsageThresholds.get(0),
            this.oldGenHeapUsageThresholds.get(1),
            this.oldGenHeapUsageThresholds.get(2)
        );
    this.usageBucketThresholds = new JvmUsageBucketThresholds(youngGenHeapPromotionRateThresholds,
        oldGenHeapUsageThresholds);
    return usageBucketThresholds;
  }

  @Override
  protected HotNodeSummary generateNodeSummary(NodeKey nodeKey) {
    HotNodeSummary nodeSummary = null;
    long timestamp = clock.millis();
    // for each RCA type this cluster RCA subscribes, read its most recent flowunit and if it is
    // unhealthy, append this flowunit to output node summary
    for (Rca<ResourceFlowUnit<HotNodeSummary>> nodeRca : this.nodeRcas) {
      // skip if we haven't receive any flowunit from this RCA yet.
      if (nodeTable.get(nodeKey, nodeRca.name()) == null) {
        continue;
      }
      List<ResourceFlowUnit<HotNodeSummary>> flowUnits =
          ImmutableList.copyOf(nodeTable.get(nodeKey, nodeRca.name()));
      List<HotResourceSummary> oldGenSummaries = new ArrayList<>();
      List<HotResourceSummary> youngGenSummaries = new ArrayList<>();
      for (ResourceFlowUnit<HotNodeSummary> flowUnit : flowUnits) {
        // skip this flowunit if :
        // 1. the timestamp of this flowunit expires
        // 2. flowunit is healthy
        // 3. flowunit does not have summary attached to it
        if (timestamp - flowUnit.getTimeStamp() > expirationTimeWindow
            || flowUnit.getResourceContext().isHealthy()
            || flowUnit.getSummary() == null) {
          continue;
        }
        if (flowUnit.getResourceContext().getState() == Resources.State.UNHEALTHY) {
          HotNodeSummary currentNodeSummary = flowUnit.getSummary();
          for (HotResourceSummary resourceSummary : currentNodeSummary.getHotResourceSummaryList()) {
            if (resourceSummary.getResource().getResourceEnum() == ResourceEnum.YOUNG_GEN) {
              youngGenSummaries.add(resourceSummary);
            }
            else if (resourceSummary.getResource().getResourceEnum() == ResourceEnum.OLD_GEN) {
              oldGenSummaries.add(resourceSummary);
            }
          }
        }
      }
      // youngGenSummaries can have multiple elements but we will only consider it as unhealthy if
      // three consecutive summaries are all unhealthy and we will then pick the first element as the summary for output.
      if (youngGenSummaries.size() >= UNHEALTHY_FLOWUNIT_THRESHOLD
          || oldGenSummaries.size() >= UNHEALTHY_FLOWUNIT_THRESHOLD) {
        if (nodeSummary == null) {
          nodeSummary = new HotNodeSummary(nodeKey.getNodeId(), nodeKey.getHostAddress());
        }
        if (youngGenSummaries.size() >= UNHEALTHY_FLOWUNIT_THRESHOLD) {
          nodeSummary.appendNestedSummary(youngGenSummaries.get(0));
        }
        if (oldGenSummaries.size() >= UNHEALTHY_FLOWUNIT_THRESHOLD) {
          nodeSummary.appendNestedSummary(oldGenSummaries.get(0));
        }
      }
    }
    return nodeSummary;
  }

  @Override
  public ResourceFlowUnit<HotClusterSummary> operate() {
    ResourceFlowUnit<HotClusterSummary> ret = super.operate();
    if (ret.getResourceContext().getState().equals(State.UNHEALTHY)) {
      PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
          RcaVerticesMetrics.NUM_HIGH_HEAP_CLUSTER_RCA_TRIGGERED, "", 1);
    }
    return ret;
  }
}
