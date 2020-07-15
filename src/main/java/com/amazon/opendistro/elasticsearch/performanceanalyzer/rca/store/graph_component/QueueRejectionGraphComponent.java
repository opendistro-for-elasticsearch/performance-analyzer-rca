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
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_LOCUS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.Constants.EVALUATION_INTERVAL_SECONDS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.Constants.RCA_PERIOD;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_RejectedReqs;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.threadpool.QueueRejectionRca;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueueRejectionGraphComponent implements AnalysisGraphComponent {
  private ElasticSearchAnalysisGraph analysisGraph;

  public QueueRejectionGraphComponent(final ElasticSearchAnalysisGraph analysisGraph) {
    this.analysisGraph = analysisGraph;
  }

  @Override
  public List<Metric> getMetrics() {
    return new ArrayList<Metric>() {
      {
        add(new ThreadPool_RejectedReqs(EVALUATION_INTERVAL_SECONDS));
      }
    };
  }

  @Override
  public void registerRca() {
    final Metric threadPoolRejectedReqs =
        analysisGraph.getMetric(AllMetrics.ThreadPoolValue.THREADPOOL_REJECTED_REQS.name()).get();

    // Node level queue rejection RCA
    QueueRejectionRca queueRejectionNodeRca = new QueueRejectionRca(RCA_PERIOD, threadPoolRejectedReqs);
    queueRejectionNodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    queueRejectionNodeRca.addAllUpstreams(Collections.singletonList(threadPoolRejectedReqs));
    analysisGraph.getOrRegisterRca(queueRejectionNodeRca);

    // Cluster level queue rejection RCA
    QueueRejectionClusterRca queueRejectionClusterRca = new QueueRejectionClusterRca(RCA_PERIOD, queueRejectionNodeRca);
    queueRejectionClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    queueRejectionClusterRca.addAllUpstreams(Collections.singletonList(queueRejectionNodeRca));
    analysisGraph.getOrRegisterRca(queueRejectionClusterRca);
  }
}
