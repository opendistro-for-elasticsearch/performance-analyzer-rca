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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscribeResponseHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util.ClusterUtils;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Task to send out a subscription request.
 */
public abstract class SubscriptionTxTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(SubscriptionTxTask.class);

  /**
   * The client object to make RPC with.
   */
  protected final NetClient netClient;

  /**
   * The encapsulated subscribe message.
   */
  protected final IntentMsg intentMsg;

  /**
   * The subscription manager to update metadata.
   */
  protected final SubscriptionManager subscriptionManager;

  /**
   * The node state manager to start/update tracking staleness.
   */
  protected final NodeStateManager nodeStateManager;

  public SubscriptionTxTask(
      final NetClient netClient,
      final IntentMsg intentMsg,
      final SubscriptionManager subscriptionManager,
      final NodeStateManager nodeStateManager) {
    this.netClient = netClient;
    this.intentMsg = intentMsg;
    this.subscriptionManager = subscriptionManager;
    this.nodeStateManager = nodeStateManager;
  }

  protected void sendSubscribeRequest(final String remoteHost, final String requesterVertex,
      final String destinationVertex, final Map<String, String> tags) {
    LOG.debug("rca: [sub-tx]: {} -> {} to {}", requesterVertex, destinationVertex, remoteHost);
    final SubscribeMessage subscribeMessage = SubscribeMessage.newBuilder()
                                                              .setDestinationNode(destinationVertex)
                                                              .setRequesterNode(requesterVertex)
                                                              .putTags("locus", tags.get("locus"))
                                                              .putTags("requester",
                                                                  ClusterUtils
                                                                      .getCurrentNodeHostAddress())
                                                              .build();
    netClient.subscribe(remoteHost, subscribeMessage,
        new SubscribeResponseHandler(subscriptionManager, nodeStateManager, remoteHost,
            destinationVertex));
    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR
        .updateStat(RcaGraphMetrics.RCA_NODES_SUB_REQ_COUNT,
            requesterVertex + ":" + destinationVertex, 1);
  }
}
