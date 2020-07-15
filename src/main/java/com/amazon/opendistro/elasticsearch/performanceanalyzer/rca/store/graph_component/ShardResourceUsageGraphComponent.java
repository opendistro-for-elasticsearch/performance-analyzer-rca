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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_AGGREGATE_UPSTREAM;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_LOCUS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.Constants.EVALUATION_INTERVAL_SECONDS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.Constants.RCA_PERIOD;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IO_TotThroughput;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IO_TotalSyscallRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HotShardClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HotShardRca;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ShardResourceUsageGraphComponent implements AnalysisGraphComponent {
  private ElasticSearchAnalysisGraph analysisGraph;

  public ShardResourceUsageGraphComponent(final ElasticSearchAnalysisGraph analysisGraph) {
    this.analysisGraph = analysisGraph;
  }

  @Override
  public List<Metric> getMetrics() {
    return new ArrayList<Metric>() {
      {
        add(new CPU_Utilization(EVALUATION_INTERVAL_SECONDS));
        add(new IO_TotThroughput(EVALUATION_INTERVAL_SECONDS));
        add(new IO_TotalSyscallRate(EVALUATION_INTERVAL_SECONDS));
      }
    };
  }

  @Override
  public void registerRca() {
    final Metric cpuUtilization = analysisGraph.getMetric(AllMetrics.OSMetrics.Constants.CPU_VALUE).get();
    final Metric ioTotThroughput = analysisGraph.getMetric(AllMetrics.OSMetrics.Constants.TOTAL_THROUGHPUT_VALUE).get();
    final Metric ioTotSysCallRate =
            analysisGraph.getMetric(AllMetrics.OSMetrics.Constants.TOTAL_SYSCALL_RATE_VALUE).get();

    // High CPU Utilization RCA
    final HotShardRca hotShardRca =
        new HotShardRca(
            EVALUATION_INTERVAL_SECONDS,
            RCA_PERIOD,
            cpuUtilization,
            ioTotThroughput,
            ioTotSysCallRate);
    hotShardRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    hotShardRca.addAllUpstreams(Arrays.asList(cpuUtilization, ioTotThroughput, ioTotSysCallRate));
    analysisGraph.getOrRegisterRca(hotShardRca);

    // Hot Shard Cluster RCA which consumes the above
    final HotShardClusterRca hotShardClusterRca = new HotShardClusterRca(RCA_PERIOD, hotShardRca);
    hotShardClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    hotShardClusterRca.addAllUpstreams(Collections.singletonList(hotShardRca));
    hotShardClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);
    analysisGraph.getOrRegisterRca(hotShardClusterRca);
  }
}
