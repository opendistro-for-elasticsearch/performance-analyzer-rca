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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.CpuClusterResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;

public class HighCpuClusterRca extends BaseClusterRca {
  public static final String RCA_TABLE_NAME = HighCpuClusterRca.class.getSimpleName();

  @SafeVarargs
  public <R extends Rca<ResourceFlowUnit<HotNodeSummary>>> HighCpuClusterRca(
      int rcaPeriod, R... nodeRca) {
    super(rcaPeriod, nodeRca);
    setCollectFromMasterNode(true);
    this.sendHealthyFlowUnits = true;
  }

  @Override
  protected CpuClusterResourceFlowUnit generateFlowUnit(HotClusterSummary clusterSummary,
      ResourceContext context) {
    if (clusterSummary == null && context == null) {
      return new CpuClusterResourceFlowUnit(clock.millis());
    }
    return new CpuClusterResourceFlowUnit(clock.millis(), context, clusterSummary, rcaConf,true);
  }
}
