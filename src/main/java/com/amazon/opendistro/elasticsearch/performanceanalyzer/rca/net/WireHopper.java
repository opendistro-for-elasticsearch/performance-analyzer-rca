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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.UnicastIntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.NetPersistor;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WireHopper {

  private static final Logger LOG = LogManager.getLogger(WireHopper.class);
  private static final int MS_IN_S = 1000;

  private final NetPersistor persistor;
  private final NetClient netClient;
  private final SubscriptionManager subscriptionManager;
  private final NodeStateManager nodeStateManager;
  private final Sender sender;
  private final Receiver receiver;
  private final SubscriptionSender subscriptionSender;

  public WireHopper(
      final NetPersistor persistor,
      final NodeStateManager nodeStateManager,
      final NetClient netClient,
      final SubscriptionManager subscriptionManager,
      final Sender sender,
      final Receiver receiver,
      final SubscriptionSender subscriptionSender) {
    this.persistor = persistor;
    this.netClient = netClient;
    this.subscriptionManager = subscriptionManager;
    this.nodeStateManager = nodeStateManager;
    this.sender = sender;
    this.receiver = receiver;
    this.subscriptionSender = subscriptionSender;
  }

  public void sendIntent(IntentMsg msg) {
    LOG.info("kk: Q-ing message for broadcast: {}", msg);
   subscriptionSender.enqueueForBroadcast(msg);
  }

  public void sendData(DataMsg dataMsg) {
    sender.enqueue(dataMsg);
  }

  public List<FlowUnitMessage> readFromWire(Node<?> node) {
    final String nodeName = node.name();
    final long intervalInSeconds = node.getEvaluationIntervalSeconds();
    final List<FlowUnitMessage> remoteFlowUnits = receiver.getFlowUnitsForNode(nodeName);
    final Set<String> publisherSet = subscriptionManager.getPublishersForNode(nodeName);

    if (remoteFlowUnits.size() < publisherSet.size()) {
      for (final String publisher : publisherSet) {
        long lastRxTimestamp = nodeStateManager.getLastReceivedTimestamp(nodeName, publisher);
        if (System.currentTimeMillis() - lastRxTimestamp > 2 * intervalInSeconds * MS_IN_S) {
          LOG.info(
              "kk: {} hasn't published in a while.. nothing from the last {} intervals",
              publisher,
              (System.currentTimeMillis() - lastRxTimestamp) / (intervalInSeconds * MS_IN_S));
          if (nodeStateManager.isRemoteHostInCluster(publisher)) {
            subscriptionSender.enqueueForUnicast(new UnicastIntentMsg("", nodeName, node.getTags(),
                publisher));
          }
        }
      }
    }
    return remoteFlowUnits;
  }

}
