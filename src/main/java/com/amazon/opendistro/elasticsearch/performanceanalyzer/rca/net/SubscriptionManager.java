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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles adding and removing of subscriptions for the vertices in the analysis graph.
 */
public class SubscriptionManager {

  private static final Logger LOG = LogManager.getLogger(SubscriptionManager.class);

  /**
   * The connection manager instance.
   */
  private final GRPCConnectionManager connectionManager;

  /**
   * Map of vertex to a set of hosts that are publishing flow units for that vertex.
   */
  private final ConcurrentMap<String, Set<String>> publisherMap = new ConcurrentHashMap<>();

  /**
   * Map of vertex to a set of hosts that are interested in consuming the flow units for that
   * vertex.
   */
  private final ConcurrentMap<String, Set<String>> subscriberMap = new ConcurrentHashMap<>();

  /**
   * The current locus of the node.
   */
  private volatile String currentLocus;

  public SubscriptionManager(
      final GRPCConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  /**
   * Unsubscribe from the flow units for upstream vertex from the host. Callers: Flow unit sender
   * thread.
   *
   * @param graphNode  The vertex whose flow units we are not interested in.
   * @param remoteHost The host from which we don't want the flow units for vertex.
   */
  public void unsubscribeAndTerminateConnection(final String graphNode, final String remoteHost) {
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
  }

  /**
   * Adds a new host as a subscriber to a vertex. Caller: subscription receiver thread.
   *
   * @param graphNode             The vertex to which the host wants to subscribe to.
   * @param subscriberHostAddress The host that wants to subscribe.
   * @param loci                  The locus which the subscribing host is interested in.
   * @return A SubscriptionStatus protobuf message that contains the status for the subscription
   *         request.
   */
  public synchronized SubscriptionStatus addSubscriber(
      final String graphNode, final String subscriberHostAddress, final String loci) {
    final List<String> vertexLoci = Arrays.asList(loci.split(RcaTagConstants.SEPARATOR));
    if (!vertexLoci.contains(currentLocus)) {
      LOG.debug("locus mismatch. Rejecting subscription. Req: {}, Curr: {}", loci, currentLocus);
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

  /**
   * Check if a vertex has downstream subscribers. Callers: flow unit send thread.
   *
   * @param graphNode The vertex that needs to check if it has downstream subscribers.
   * @return true if it has, false otherwise.
   */
  public boolean isNodeSubscribed(final String graphNode) {
    // happens-before: reading from a java.util.concurrent collection which guarantees read
    // reflects most recent completed update.
    return subscriberMap.containsKey(graphNode);
  }

  /**
   * Get subscribers for a vertex. Callers: flow unit send thread.
   *
   * @param graphNode The vertex whose subscribers need to be returned.
   * @return The set of host addresses that are the downstream subscribers.
   */
  public ImmutableSet<String> getSubscribersFor(final String graphNode) {
    // happens-before: ImmutableSet - final field semantics. Reading from java.util.concurrent
    // collection.
    return ImmutableSet.copyOf(subscriberMap.getOrDefault(graphNode, new HashSet<>()));
  }

  /**
   * Adds a host address as a publisher for a vertex. Callers: subscription handler thread.
   *
   * @param graphNode            The vertex for which a publisher is being added.
   * @param publisherHostAddress The host address of the publisher node.
   */
  public synchronized void addPublisher(final String graphNode, final String publisherHostAddress) {
    LOG.info("Added publisher: {} for graphNode: {}", publisherHostAddress, graphNode);

    final Set<String> currentPublishers = publisherMap.getOrDefault(graphNode, new HashSet<>());
    currentPublishers.add(publisherHostAddress);
    publisherMap.put(graphNode, currentPublishers);
  }

  public void setCurrentLocus(String currentLocus) {
    this.currentLocus = currentLocus;
  }

  public Set<String> getPublishersForNode(String graphNode) {
    return publisherMap.getOrDefault(graphNode, Collections.emptySet());
  }
}
