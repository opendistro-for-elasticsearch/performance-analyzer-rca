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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.google.common.collect.ImmutableSet;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SubscriptionManager {

  private static final Logger LOG = LogManager.getLogger(SubscriptionManager.class);

  private final GRPCConnectionManager connectionManager;
  private final NetClient netClient;

  private ConcurrentMap<String, Set<String>> publisherMap = new ConcurrentHashMap<>();
  private ConcurrentMap<String, Set<String>> subscriberMap = new ConcurrentHashMap<>();

  private volatile String currentLocus;

  public SubscriptionManager(
      final GRPCConnectionManager connectionManager, final NetClient netClient) {
    this.connectionManager = connectionManager;
    this.netClient = netClient;
  }

  public void broadcastSubscribeRequest(
      final String requesterGraphNode,
      final String destinationGraphNode,
      final Map<String, String> tags) {
    final List<String> remoteHosts = connectionManager.getAllRemoteHosts();

    for (final String remoteHost : remoteHosts) {
      sendSubscribeRequest(remoteHost, requesterGraphNode, destinationGraphNode, tags);
    }
  }

  public void sendSubscribeRequest(
      final String remoteHost,
      final String requesterGraphNode,
      final String destinationGraphNode,
      final Map<String, String> tags) {
    LOG.debug(
        "Sending subscribe message to: {}. Need {} to compute {}",
        remoteHost,
        destinationGraphNode,
        requesterGraphNode);
    netClient.subscribe(
        remoteHost,
        buildSubscribeMessage(requesterGraphNode, destinationGraphNode, tags),
        new SubscriptionResponseHandler2(remoteHost, destinationGraphNode));
  }

  public void unsubscribe(final String graphNode, final String remoteHost) {
    LOG.debug("Unsubscribing {} from {} updates", remoteHost, graphNode);

    if (subscriberMap.containsKey(graphNode)) {
      final Set<String> subscribers = subscriberMap.get(graphNode);
      subscribers.remove(remoteHost);
      if (subscribers.size() > 0) {
        subscriberMap.put(graphNode, subscribers);
      } else {
        subscriberMap.remove(graphNode);
      }
    }
    connectionManager.terminateConnection(remoteHost);
    netClient.flushStream(remoteHost);
  }

  private SubscribeMessage buildSubscribeMessage(
      final String requesterGraphNode,
      final String destinationGraphNode,
      final Map<String, String> tags) {
    return SubscribeMessage.newBuilder()
                           .setRequesterNode(requesterGraphNode)
                           .setDestinationNode(destinationGraphNode)
                           .putTags("locus", tags.get("locus"))
                           .putTags("requester", connectionManager.getCurrentHostAddress())
                           .build();
  }

  public synchronized SubscriptionStatus addSubscriber(
      final String graphNode, final String subscriberHostAddress, final String locus) {
    if (!currentLocus.equals(locus)) {
      LOG.debug("locus mismatch. Rejecting subscription. Req: {}, Curr: {}", locus, currentLocus);
      return SubscriptionStatus.TAG_MISMATCH;
    }

    Set<String> currentSubscribers = subscriberMap.getOrDefault(graphNode, new HashSet<>());
    currentSubscribers.add(subscriberHostAddress);
    // happens-before: update to a java.util.concurrent collection. Updated value will be visible
    // to subsequent reads.
    subscriberMap.put(graphNode, currentSubscribers);

    LOG.debug("locus matched. Added subscriber {} for {}", subscriberHostAddress, graphNode);
    return SubscriptionStatus.SUCCESS;
  }

  public boolean isNodeSubscribed(final String graphNode) {
    // happens-before: reading from a java.util.concurrent collection which guarantees read
    // reflects most recent completed update.
    return subscriberMap.containsKey(graphNode);
  }

  public ImmutableSet<String> getSubscribersFor(final String graphNode) {
    // happens-before: ImmutableSet - final field semantics. Reading from java.util.concurrent
    // collection.
    return ImmutableSet.copyOf(subscriberMap.getOrDefault(graphNode, new HashSet<>()));
  }

  public synchronized void addPublisher(final String graphNode, final String publisherHostAddress) {
    LOG.info("Added publisher: {} for graphNode: {}", publisherHostAddress, graphNode);

    final Set<String> currentPublishers = publisherMap.getOrDefault(graphNode, new HashSet<>());
    currentPublishers.add(publisherHostAddress);
    publisherMap.put(graphNode, currentPublishers);
  }

  public void dumpStats() {
    LOG.debug("Subscribers: {}", subscriberMap);
    LOG.debug("Publishers: {}", publisherMap);

    connectionManager.dumpStats();
    netClient.dumpStreamStats();
  }

  public void setCurrentLocus(String currentLocus) {
    this.currentLocus = currentLocus;
  }

  public Set<String> getPublishersForNode(String graphNode) {
    return publisherMap.get(graphNode);
  }

  private class SubscriptionResponseHandler2 implements StreamObserver<SubscribeResponse> {

    private final String remoteHost;
    private final String graphNode;

    SubscriptionResponseHandler2(final String remoteHost, final String graphNode) {
      this.remoteHost = remoteHost;
      this.graphNode = graphNode;
    }

    @Override
    public void onNext(SubscribeResponse subscribeResponse) {
      if (subscribeResponse.getSubscriptionStatus() == SubscriptionStatus.SUCCESS) {
        LOG.debug("Publisher ack'd: {}", remoteHost);
        addPublisher(graphNode, remoteHost);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("Publisher threw an error: {}", throwable.getMessage());
      throwable.printStackTrace();
    }

    @Override
    public void onCompleted() {
      LOG.debug("Finished subscription request for {}", remoteHost);
    }
  }
}
