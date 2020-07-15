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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_LOCUS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.Constants.EVALUATION_INTERVAL_SECONDS;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Collator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Publisher;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.QueueHealthDecider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import java.util.Arrays;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AnalysisGraphDecisionMakerManager {
  private static final Logger LOG = LogManager.getLogger(AnalysisGraphDecisionMakerManager.class);

  private ElasticSearchAnalysisGraph analysisGraph;

  AnalysisGraphDecisionMakerManager(final ElasticSearchAnalysisGraph analysisGraph) {
    this.analysisGraph = analysisGraph;
  }

  void registerDecisionMaker() {
    final QueueRejectionClusterRca queueRejectionClusterRca =
        (QueueRejectionClusterRca)
            analysisGraph.getRca(QueueRejectionClusterRca.class.getSimpleName()).get();

    // Queue Health Decider
    QueueHealthDecider queueHealthDecider =
        new QueueHealthDecider(EVALUATION_INTERVAL_SECONDS, 12, queueRejectionClusterRca);
    queueHealthDecider.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    queueHealthDecider.addAllUpstreams(Collections.singletonList(queueRejectionClusterRca));

    // Collator - Collects actions from all deciders and aligns impact vectors
    Collator collator = new Collator(EVALUATION_INTERVAL_SECONDS, queueHealthDecider);
    collator.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    collator.addAllUpstreams(Arrays.asList(queueHealthDecider));

    // Publisher - Executes decisions output from collator
    Publisher publisher = new Publisher(EVALUATION_INTERVAL_SECONDS, collator);
    publisher.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    publisher.addAllUpstreams(Collections.singletonList(collator));
  }
}
