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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.InterNodeRpcServiceGrpc;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.InterNodeRpcServiceGrpc.InterNodeRpcServiceStub;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class that manages the channel to other hosts in the cluster. It also performs staleness checks,
 * and initiates a new connection if it deems a channel to have gone stale.
 *
 * <p>It also has listens to cluster state changes and manages handling connections to the changed
 * hosts.
 */
public class GRPCConnectionManager {
  private static final Logger LOG = LogManager.getLogger(GRPCConnectionManager.class);
  private static final String EMPTY_STRING = "";

  private ConcurrentMap<String, AtomicReference<ManagedChannel>> perHostChannelMap =
      new ConcurrentHashMap<>();
  private ConcurrentMap<String, AtomicReference<InterNodeRpcServiceStub>> perHostClientStubMap =
      new ConcurrentHashMap<>();

  private final boolean shouldUseHttps;

  public GRPCConnectionManager(final boolean shouldUseHttps) {
    this.shouldUseHttps = shouldUseHttps;
  }

  public InterNodeRpcServiceStub getClientStubForHost(
      final String remoteHost) {
    if (perHostClientStubMap.containsKey(remoteHost)) {
      return perHostClientStubMap.get(remoteHost).get();
    }
    return addOrUpdateClientStubForHost(remoteHost);
  }

  private synchronized InterNodeRpcServiceStub addOrUpdateClientStubForHost(final String remoteHost) {
    final InterNodeRpcServiceStub stub = buildStubForHost(remoteHost);
    AtomicReference<InterNodeRpcServiceStub> existingStub = perHostClientStubMap.get(remoteHost);
    if (existingStub == null) {
      // happens-before: updating java.util.concurrent collection. Update will be made visible to
      // all threads.
      perHostClientStubMap.put(remoteHost, new AtomicReference<>(stub));
    } else {
      // happens-before: updating AtomicReference. Reads on this AtomicReference will reflect
      // updated value in all threads.
      perHostClientStubMap.get(remoteHost).set(stub);
    }
    return stub;
  }

  public void shutdown() {
    removeAllStubs();
    terminateAllConnections();
  }

  /**
   * Read the NodeDetails of all the remote nodes
   * skip the first node in the list because it is local node that
   * this is currently running on.
   */
  public List<String> getAllRemoteHosts() {
    return ClusterDetailsEventProcessor.getNodesDetails().stream()
        .skip(1)
        .map(node -> node.getHostAddress())
        .collect(Collectors.toList());
  }

  public String getCurrentHostAddress() {
    final List<ClusterDetailsEventProcessor.NodeDetails> nodes = ClusterDetailsEventProcessor
        .getNodesDetails();
    if (nodes.size() > 0) {
      return nodes.get(0).getHostAddress();
    }

    // TODO: Maybe fallback on InetAddress.getCurrentHostAddress() method instead of returning empty
    // string.
    return EMPTY_STRING;
  }

  private ManagedChannel getChannelForHost(final String remoteHost) {
    if (perHostChannelMap.containsKey(remoteHost)) {
      return perHostChannelMap.get(remoteHost).get();
    }

    return addOrUpdateChannelForHost(remoteHost);
  }

  private synchronized ManagedChannel addOrUpdateChannelForHost(final String remoteHost) {
    final ManagedChannel channel = buildChannelForHost(remoteHost);
    if (perHostChannelMap.get(remoteHost) == null) {
      final AtomicReference<ManagedChannel> managedChannelAtomicReference = new AtomicReference<>(
          channel);
      // happens-before: updating java.util.concurrent collection. Update will be made visible to
      // all subsequent reads.
      perHostChannelMap.put(remoteHost, managedChannelAtomicReference);
    } else {
      // happens-before: updating the AtomicReference in the map. Update will be visible to all
      // threads for subsequent gets.
      perHostChannelMap.get(remoteHost).set(channel);
    }
    return channel;
  }

  private ManagedChannel buildChannelForHost(final String remoteHost) {
    return shouldUseHttps ? buildSecureChannel(remoteHost) : buildInsecureChannel(remoteHost);
  }

  private ManagedChannel buildInsecureChannel(final String remoteHost) {
    return ManagedChannelBuilder.forAddress(remoteHost, Util.RPC_PORT).usePlaintext().build();
  }

  private ManagedChannel buildSecureChannel(final String remoteHost) {
    try {
      return NettyChannelBuilder.forAddress(remoteHost, Util.RPC_PORT)
          .sslContext(
              GrpcSslContexts.forClient()
                  .trustManager(InsecureTrustManagerFactory.INSTANCE)
                  .build())
          .build();
    } catch (SSLException e) {
      LOG.error("@@: Unable to build an SSL gRPC client. Exception: {}", e.getMessage());
      e.printStackTrace();

      // Wrap the SSL Exception in a generic RTE and re-throw.
      throw new RuntimeException(e);
    }
  }

  private InterNodeRpcServiceStub buildStubForHost(
      final String remoteHost) {
    return InterNodeRpcServiceGrpc.newStub(getChannelForHost(remoteHost));
  }

  private void removeAllStubs() {
    for (Map.Entry<String, AtomicReference<InterNodeRpcServiceStub>> entry :
        perHostClientStubMap.entrySet()) {
      LOG.debug("Removing client stub for host: {}", entry.getKey());
      perHostClientStubMap.remove(entry.getKey());
    }
  }

  private void terminateAllConnections() {
    for (Map.Entry<String, AtomicReference<ManagedChannel>> entry : perHostChannelMap.entrySet()) {
      LOG.debug("shutting down connection to host: {}", entry.getKey());
      entry.getValue().get().shutdownNow();
      perHostChannelMap.remove(entry.getKey());
    }
  }

  public void terminateConnection(String remoteHost) {
    perHostClientStubMap.remove(remoteHost);
    perHostChannelMap.remove(remoteHost);
  }

  public void dumpStats() {
    LOG.debug("Stubs: {}", perHostClientStubMap);
    LOG.debug("Channels: {}", perHostChannelMap);
  }
}
