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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util.ClusterUtils;
import java.util.Map;

/**
 * Task that broadcasts a subscription request to the current node's peers.
 */
public class BroadcastSubscriptionTxTask extends SubscriptionTxTask {
  public BroadcastSubscriptionTxTask(
      NetClient netClient,
      IntentMsg intentMsg,
      SubscriptionManager subscriptionManager,
      NodeStateManager nodeStateManager,
      final AppContext appContext) {
    super(netClient, intentMsg, subscriptionManager, nodeStateManager, appContext);
  }

  /**
   * Broadcasts a subscription request to all the peers in the cluster.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    final String requesterVertex = intentMsg.getRequesterNode();
    final String destinationVertex = intentMsg.getDestinationNode();
    final Map<String, String> tags = intentMsg.getRcaConfTags();

    for (final String remoteHost : getPeerIps()) {
      sendSubscribeRequest(remoteHost, requesterVertex, destinationVertex, tags);
    }
  }
}
