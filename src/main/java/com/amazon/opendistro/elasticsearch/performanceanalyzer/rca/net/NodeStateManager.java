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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util.ClusterUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the subscription state for the nodes in the graph.
 */
public class NodeStateManager {

  private static final String SEPARATOR = ".";

  /**
   * Map of host address of a remote node to the last time we received a flow unit from that node.
   */
  private final ConcurrentMap<String, Long> lastReceivedTimestampMap = new ConcurrentHashMap<>();

  /**
   * Map of host address to the current subscription status of that host.
   */
  private final ConcurrentMap<String, AtomicReference<SubscriptionStatus>> subscriptionStatusMap =
      new ConcurrentHashMap<>();

  /**
   * Updates the timestamp for the composite key: (host, vertex) marking when the last successful
   * flow unit reception happened.
   *
   * @param host      The host that sent the flow unit.
   * @param graphNode The vertex for which the flow unit was sent for.
   * @param timestamp The timestamp at which we received.
   */
  public void updateReceiveTime(final String host, final String graphNode, final long timestamp) {
    final String compositeKey = graphNode + SEPARATOR + host;
    lastReceivedTimestampMap.put(compositeKey, timestamp);
  }

  /**
   * Retrieves the latest timestamp at which we received a flow unit from this host for this
   * vertex.
   *
   * @param graphNode The vertex for which we need the last received time stamp for.
   * @param host      The host for which we need the last received timestamp for, for the vertex.
   * @return The timestamp at which we received a flow unit from the host for the vertex if present,
   *         a timestamp in the distant past(0) otherwise.
   */
  public long getLastReceivedTimestamp(String graphNode, String host) {
    final String compositeKey = graphNode + SEPARATOR + host;
    // Return the last received value or a value that is in the distant past.
    return lastReceivedTimestampMap.getOrDefault(compositeKey, 0L);
  }

  @VisibleForTesting
  SubscriptionStatus getSubscriptionStatus(String graphNode, String host) {
    final String compositeKey = graphNode + SEPARATOR + host;
    // Return the last received value or a value that is in the distant past.
    AtomicReference<SubscriptionStatus> ref = subscriptionStatusMap.get(compositeKey);
    if (ref == null) {
      return null;
    }
    return ref.get();
  }

  /**
   * Updates the subscription status of a host for a vertex.
   *
   * @param graphNode The vertex.
   * @param host      The host.
   * @param status    The subscription status.
   */
  public synchronized void updateSubscriptionState(final String graphNode, final String host, final
  SubscriptionStatus status) {
    final String compositeKey = graphNode + SEPARATOR + host;
    subscriptionStatusMap.putIfAbsent(compositeKey, new AtomicReference<>());
    subscriptionStatusMap.get(compositeKey).set(status);
  }

  /**
   * Gets a list of hosts that have not recently published flow units for the vertex. It also
   * includes new nodes that have come up since the last discovery that we have not yet tried
   * subscribing to.
   *
   * @param graphNode       The vertex for which we need flow units from remote nodes.
   * @param maxIdleDuration the max time delta till which we wait for flow units from a host.
   * @param publishers      A set of known publishers for the current vertex.
   * @return a list of hosts that we need to subscribe to.
   */
  public ImmutableList<String> getStaleOrNotSubscribedNodes(final String graphNode,
      final long maxIdleDuration, Set<String> publishers) {
    final long currentTime = System.currentTimeMillis();
    final Set<String> hostsToSubscribeTo = new HashSet<>();
    for (final String publisher : publishers) {
      long lastRxTimestamp = getLastReceivedTimestamp(graphNode, publisher);
      if (lastRxTimestamp > 0 && currentTime - lastRxTimestamp > maxIdleDuration && ClusterUtils
          .isHostAddressInCluster(publisher)) {
        hostsToSubscribeTo.add(publisher);
      }
    }

    final List<String> peers = ClusterUtils.getAllPeerHostAddresses();
    if (peers != null) {
      for (final String peerHost : peers) {
        String compositeKey = graphNode + SEPARATOR + peerHost;
        if (!subscriptionStatusMap.containsKey(compositeKey)) {
          hostsToSubscribeTo.add(peerHost);
        }
      }
    }

    return ImmutableList.copyOf(hostsToSubscribeTo);
  }
}
