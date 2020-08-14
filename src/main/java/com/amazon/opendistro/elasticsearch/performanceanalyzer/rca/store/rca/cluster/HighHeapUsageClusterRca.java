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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.JvmClusterResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;

/**
 * This RCA runs on the elected master only and it subscribes all high heap RCA from data nodes
 * within the entire cluster. This can help to reduce the network bandwidth/workload on master node
 * and push computation related workload on data node itself. The RCA uses a cache to keep track of
 * the last three metrics from each node and will mark the node as unhealthy if the last three
 * consecutive flowunits are unhealthy. And if any node is unthleath, the entire cluster will be
 * considered as unhealthy and send out corresponding flowunits to downstream nodes.
 */
public class HighHeapUsageClusterRca extends BaseClusterRca {
  public static final String RCA_TABLE_NAME = HighHeapUsageClusterRca.class.getSimpleName();

  public <R extends Rca<ResourceFlowUnit<HotNodeSummary>>> HighHeapUsageClusterRca(
      final int rcaPeriod,
      final R hotNodeRca) {
    super(rcaPeriod, hotNodeRca);
    setCollectFromMasterNode(false);
    this.sendHealthyFlowUnits = true;
  }

  @Override
  public void readRcaConf(RcaConf conf) {
    super.readRcaConf(conf);
  }

  @Override
  protected JvmClusterResourceFlowUnit generateFlowUnit(HotClusterSummary clusterSummary,
      ResourceContext context) {
    if (clusterSummary == null && context == null) {
      return new JvmClusterResourceFlowUnit(clock.millis());
    }
    return new JvmClusterResourceFlowUnit(clock.millis(), context, clusterSummary, rcaConf,true);
  }
}
