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

  private Map<String, ManagedChannel> perHostChannelMap = new HashMap<>();
  private Map<String, InterNodeRpcServiceGrpc.InterNodeRpcServiceStub> perHostClientStubMap =
      new HashMap<>();

  private final boolean shouldUseHttps;

  public GRPCConnectionManager(final boolean shouldUseHttps) {
    this.shouldUseHttps = shouldUseHttps;
  }

  public InterNodeRpcServiceGrpc.InterNodeRpcServiceStub getClientStubForHost(
      final String remoteHost) {
    if (perHostClientStubMap.containsKey(remoteHost)) {
      return perHostClientStubMap.get(remoteHost);
    }

    final InterNodeRpcServiceGrpc.InterNodeRpcServiceStub stub = buildStubForHost(remoteHost);
    perHostClientStubMap.put(remoteHost, stub);
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
      return perHostChannelMap.get(remoteHost);
    }

    final ManagedChannel channel = buildChannelForHost(remoteHost);
    perHostChannelMap.put(remoteHost, channel);
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

  private InterNodeRpcServiceGrpc.InterNodeRpcServiceStub buildStubForHost(
      final String remoteHost) {
    return InterNodeRpcServiceGrpc.newStub(getChannelForHost(remoteHost));
  }

  private void removeAllStubs() {
    for (Map.Entry<String, InterNodeRpcServiceGrpc.InterNodeRpcServiceStub> entry :
        perHostClientStubMap.entrySet()) {
      LOG.debug("Removing client stub for host: {}", entry.getKey());
      perHostClientStubMap.remove(entry.getKey());
    }
  }

  private void terminateAllConnections() {
    for (Map.Entry<String, ManagedChannel> entry : perHostChannelMap.entrySet()) {
      LOG.debug("shutting down connection to host: {}", entry.getKey());
      entry.getValue().shutdownNow();
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
