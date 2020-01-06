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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse.PublishResponseStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.NetPersistor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
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

  public WireHopper(
      final NetPersistor persistor,
      final NodeStateManager nodeStateManager,
      final NetClient netClient,
      final SubscriptionManager subscriptionManager) {
    this.persistor = persistor;
    this.netClient = netClient;
    this.subscriptionManager = subscriptionManager;
    this.nodeStateManager = nodeStateManager;
  }

  public void sendIntent(IntentMsg msg) {
    subscriptionManager.broadcastSubscribeRequest(
        msg.getRequesterNode(), msg.getDestinationNode(), msg.getRcaConfTags());
  }

  public void sendData(DataMsg dataMsg) {
    final String sourceNode = dataMsg.getSourceNode();
    final String esNode;
    final NodeDetails currentNode = ClusterDetailsEventProcessor.getCurrentNodeDetails();
    if (currentNode != null) {
      esNode = currentNode.getHostAddress();
    } else {
      LOG.error("Could not get current host address from cluster level metrics reader.");
      esNode = "";
    }
    if (subscriptionManager.isNodeSubscribed(sourceNode)) {
      final Set<String> downstreamHostAddresses = subscriptionManager.getSubscribersFor(sourceNode);
      LOG.debug("{} has downstream subscribers: {}", sourceNode, downstreamHostAddresses);
      for (final String downstreamHostAddress : downstreamHostAddresses) {
        for (final GenericFlowUnit flowUnit : dataMsg.getFlowUnits()) {
          netClient.publish(
              downstreamHostAddress,
              flowUnit.buildFlowUnitMessage(sourceNode, esNode),
              new StreamObserver<PublishResponse>() {
                @Override
                public void onNext(final PublishResponse value) {
                  LOG.debug(
                      "rca: Received acknowledgement from the server. status: {}",
                      value.getDataStatus());
                  if (value.getDataStatus() == PublishResponseStatus.NODE_SHUTDOWN) {
                    subscriptionManager.unsubscribe(sourceNode, downstreamHostAddress);
                  }
                }

                @Override
                public void onError(final Throwable t) {
                  LOG.error("rca: Encountered an exception at the server: ", t);
                  subscriptionManager.unsubscribe(sourceNode, downstreamHostAddress);
                  // TODO: When an error happens, onCompleted is not guaranteed. So, terminate the
                  // connection.
                }

                @Override
                public void onCompleted() {
                  LOG.debug("rca: Server closed the data channel!");
                }
              });
        }
      }
    } else {
      LOG.debug("No subscribers for {}.", sourceNode);
    }
  }

  public List<FlowUnitWrapper> readFromWire(Node<?> node) {
    final String nodeName = node.name();
    final long intervalInSeconds = node.getEvaluationIntervalSeconds();
    final List<FlowUnitWrapper> remoteFlowUnits = persistor.read(nodeName);
    final Set<String> publisherSet = subscriptionManager.getPublishersForNode(nodeName);

    if (remoteFlowUnits.size() < publisherSet.size()) {
      for (final String publisher : publisherSet) {
        long lastRxTimestamp = nodeStateManager.getLastReceivedTimestamp(nodeName, publisher);
        if (System.currentTimeMillis() - lastRxTimestamp > 2 * intervalInSeconds * MS_IN_S) {
          LOG.debug(
              "{} hasn't published in a while.. nothing from the last {} intervals",
              publisher,
              (System.currentTimeMillis() - lastRxTimestamp) / (intervalInSeconds * MS_IN_S));
          if (nodeStateManager.isRemoteHostInCluster(publisher)) {
            resendIntent(nodeName, publisher, node.getTags());
          }
        }
      }
    }
    return remoteFlowUnits;
  }

  private void resendIntent(
      final String node, final String remoteHost, final Map<String, String> tags) {
    LOG.debug("Resending subscription to {} to get {} flow units", remoteHost, node);
    subscriptionManager.sendSubscribeRequest(remoteHost, "", node, tags);
  }
}
