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
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class that manages the channel to other hosts in the cluster. It also performs staleness checks,
 * and initiates a new connection if it deems a channel to have gone stale.
 *
 * <p>It also listens to cluster state changes and manages handling connections to the changed
 * hosts.
 */
public class GRPCConnectionManager {

  private static final Logger LOG = LogManager.getLogger(GRPCConnectionManager.class);

  /**
   * Map of remote host to a Netty channel to that host.
   */
  private ConcurrentMap<String, AtomicReference<ManagedChannel>> perHostChannelMap =
      new ConcurrentHashMap<>();

  /**
   * Map of remote host to a grpc client object of that host. The client objects are created over
   * the channels for those hosts and are used to call RPC methods on the hosts.
   */
  private ConcurrentMap<String, AtomicReference<InterNodeRpcServiceStub>> perHostClientStubMap =
      new ConcurrentHashMap<>();

  /**
   * Flag that controls if we need to use a secure or an insecure channel.
   */
  private final boolean shouldUseHttps;

  public GRPCConnectionManager(final boolean shouldUseHttps) {
    this.shouldUseHttps = shouldUseHttps;
  }

  @VisibleForTesting
  public ConcurrentMap<String, AtomicReference<ManagedChannel>> getPerHostChannelMap() {
    return perHostChannelMap;
  }

  @VisibleForTesting
  public ConcurrentMap<String, AtomicReference<InterNodeRpcServiceStub>> getPerHostClientStubMap() {
    return perHostClientStubMap;
  }

  /**
   * Gets the client stub(on which the rpcs can be initiated) for a host.
   *
   * @param remoteHost The host to which we want to make an RPC to.
   * @return The stub object.
   */
  public InterNodeRpcServiceStub getClientStubForHost(
      final String remoteHost) {
    final AtomicReference<InterNodeRpcServiceStub> stubAtomicReference =
        perHostClientStubMap.get(remoteHost);
    if (stubAtomicReference != null) {
      return stubAtomicReference.get();
    }
    return addOrUpdateClientStubForHost(remoteHost);
  }

  /**
   * Builds or updates a stub object for host. Callers: The subscription send thread, the flow unit
   * send thread.
   *
   * @param remoteHost The host to which an RPC needs to be made.
   * @return The stub object.
   */
  private synchronized InterNodeRpcServiceStub addOrUpdateClientStubForHost(
      final String remoteHost) {
    final InterNodeRpcServiceStub stub = buildStubForHost(remoteHost);
    perHostClientStubMap.computeIfAbsent(remoteHost, s -> new AtomicReference<>());
    perHostClientStubMap.get(remoteHost).set(stub);
    return stub;
  }

  public void shutdown() {
    removeAllStubs();
    terminateAllConnections();
  }

  private ManagedChannel getChannelForHost(final String remoteHost) {
    final AtomicReference<ManagedChannel> managedChannelAtomicReference = perHostChannelMap
        .get(remoteHost);
    if (managedChannelAtomicReference != null) {
      return managedChannelAtomicReference.get();
    }

    return addOrUpdateChannelForHost(remoteHost);
  }

  /**
   * Builds or updates a channel object to be used by a client stub. Callers: Send flow unit thread,
   * send subscription thread.
   *
   * @param remoteHost The host to which we want to establish a channel to.
   * @return a Managed channel object.
   */
  private synchronized ManagedChannel addOrUpdateChannelForHost(final String remoteHost) {
    final ManagedChannel channel = buildChannelForHost(remoteHost);
    perHostChannelMap.computeIfAbsent(remoteHost, s -> new AtomicReference<>());
    perHostChannelMap.get(remoteHost).set(channel);
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
                                                   .trustManager(
                                                       InsecureTrustManagerFactory.INSTANCE)
                                                   .build())
                                .build();
    } catch (SSLException e) {
      LOG.error("Unable to build an SSL gRPC client. Exception: {}", e.getMessage());
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

  /**
   * Removes the stub and the channel object for the host.
   *
   * @param remoteHost the host to which we want to terminate connection from.
   */
  public void terminateConnection(String remoteHost) {
    perHostClientStubMap.remove(remoteHost);
    perHostChannelMap.remove(remoteHost);
  }
}
