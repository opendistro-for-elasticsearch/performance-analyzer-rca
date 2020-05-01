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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.requests.CompositeSubscribeRequest;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Task that processes received subscribe messages.
 */
public class SubscriptionRxTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(SubscriptionRxTask.class);

  /**
   * The subscription manager instance to update metadata.
   */
  private final SubscriptionManager subscriptionManager;

  /**
   * The subscribe message with the response stream.
   */
  private final CompositeSubscribeRequest compositeSubscribeRequest;

  public SubscriptionRxTask(
      final SubscriptionManager subscriptionManager,
      final CompositeSubscribeRequest compositeSubscribeRequest) {
    this.subscriptionManager = subscriptionManager;
    this.compositeSubscribeRequest = compositeSubscribeRequest;
  }

  /**
   * Process the subscription request.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    final SubscribeMessage request = compositeSubscribeRequest.getSubscribeMessage();
    final Map<String, String> tags = request.getTagsMap();
    final String requesterHostAddress = tags.getOrDefault("requester", "");
    final String locus = tags.getOrDefault("locus", "");
    final SubscriptionStatus subscriptionStatus =
        subscriptionManager
            .addSubscriber(request.getDestinationNode(), requesterHostAddress, locus);

    LOG.debug("rca: [sub-rx]: {} <- {} from {} Result: {}", request.getDestinationNode(),
        request.getRequesterNode(), requesterHostAddress, subscriptionStatus);

    final StreamObserver<SubscribeResponse> responseStream = compositeSubscribeRequest
        .getSubscribeResponseStream();
    // TODO: Wrap this in a try-catch
    responseStream.onNext(SubscribeResponse.newBuilder()
                                           .setSubscriptionStatus(subscriptionStatus)
                                           .build());
    responseStream.onCompleted();
    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR
        .updateStat(RcaGraphMetrics.RCA_NODES_SUB_ACK_COUNT,
            request.getRequesterNode() + ":" + request.getDestinationNode(), 1);
  }
}
