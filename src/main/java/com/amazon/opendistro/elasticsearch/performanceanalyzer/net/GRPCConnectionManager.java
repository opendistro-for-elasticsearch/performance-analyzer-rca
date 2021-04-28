/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CertificateUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.InterNodeRpcServiceGrpc;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.InterNodeRpcServiceGrpc.InterNodeRpcServiceStub;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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
  private static final int MAX_RETRY_ATTEMPTS = 2;
  private final int port;
  // TLS certificate, private key, and trusted root CA files
  private File certFile;
  private File pkeyFile;
  private File trustedCasFile;

  /**
   * Map of remote hostId to a Netty channel to that host.
   */
  private ConcurrentMap<InstanceDetails.Id, AtomicReference<ManagedChannel>> perHostChannelMap = new ConcurrentHashMap<>();

  /**
   * Map of remote hostId to a grpc client object of that host. The client objects are created over
   * the channels for those hosts and are used to call RPC methods on the hosts.
   */
  private ConcurrentMap<InstanceDetails.Id, AtomicReference<InterNodeRpcServiceStub>> perHostClientStubMap = new ConcurrentHashMap<>();

  /**
   * Flag that controls if we need to use a secure or an insecure channel.
   */
  private final boolean shouldUseHttps;

  public GRPCConnectionManager(final boolean shouldUseHttps) {
    this.shouldUseHttps = shouldUseHttps;
    this.port = 0;
    if (shouldUseHttps) {
      this.certFile = CertificateUtils.getClientCertificateFile();
      this.pkeyFile = CertificateUtils.getClientPrivateKeyFile();
      this.trustedCasFile = CertificateUtils.getClientTrustedCasFile();
    }
  }

  /**
   * Constructor that allows you to specify which port a client should connect to
   * @param shouldUseHttps Whether to enable TLS
   * @param port The port number that client stubs should attempt to connect to
   */
  public GRPCConnectionManager(final boolean shouldUseHttps, int port) {
    this.shouldUseHttps = shouldUseHttps;
    this.port = port;
    if (shouldUseHttps) {
      this.certFile = CertificateUtils.getClientCertificateFile();
      this.pkeyFile = CertificateUtils.getClientPrivateKeyFile();
      this.trustedCasFile = CertificateUtils.getClientTrustedCasFile();
    }
  }

  @VisibleForTesting
  public ConcurrentMap<InstanceDetails.Id, AtomicReference<ManagedChannel>> getPerHostChannelMap() {
    return perHostChannelMap;
  }

  @VisibleForTesting
  public ConcurrentMap<InstanceDetails.Id, AtomicReference<InterNodeRpcServiceStub>> getPerHostClientStubMap() {
    return perHostClientStubMap;
  }

  /**
   * Gets the client stub(on which the rpcs can be initiated) for a host.
   *
   * @param remoteHost The host to which we want to make an RPC to.
   * @return The stub object.
   */
  public InterNodeRpcServiceStub getClientStubForHost(
      final InstanceDetails remoteHost) {
    final AtomicReference<InterNodeRpcServiceStub> stubAtomicReference = perHostClientStubMap.get(remoteHost.getInstanceId());
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
  private synchronized InterNodeRpcServiceStub addOrUpdateClientStubForHost(final InstanceDetails remoteHost) {
    final InterNodeRpcServiceStub stub = buildStubForHost(remoteHost);
    perHostClientStubMap.computeIfAbsent(remoteHost.getInstanceId(), s -> new AtomicReference<>());
    perHostClientStubMap.get(remoteHost.getInstanceId()).set(stub);
    return stub;
  }

  public void shutdown() {
    removeAllStubs();
    terminateAllConnections();
  }

  private ManagedChannel getChannelForHost(final InstanceDetails remoteHost) {
    final AtomicReference<ManagedChannel> managedChannelAtomicReference = perHostChannelMap.get(remoteHost.getInstanceId());
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
  private synchronized ManagedChannel addOrUpdateChannelForHost(final InstanceDetails remoteHost) {
    final ManagedChannel channel = buildChannelForHost(remoteHost);
    perHostChannelMap.computeIfAbsent(remoteHost.getInstanceId(), s -> new AtomicReference<>());
    perHostChannelMap.get(remoteHost.getInstanceId()).set(channel);
    return channel;
  }

  private ManagedChannel buildChannelForHost(final InstanceDetails remoteHost) {
    return shouldUseHttps ? buildSecureChannel(remoteHost) : buildInsecureChannel(remoteHost);
  }

  private int getPortFromHost(final InstanceDetails remoteHost) {
    int port = this.port != 0 ? this.port : remoteHost.getGrpcPort();
    if (port == -1) {
      throw new IllegalArgumentException("Invalid port for grpc: " + port);
    }
    return port;
  }

  private ManagedChannel buildInsecureChannel(final InstanceDetails remoteHost) {
    return ManagedChannelBuilder.forAddress(remoteHost.getInstanceIp().toString(),
        getPortFromHost(remoteHost))
                                .usePlaintext()
                                .enableRetry()
                                .maxRetryAttempts(MAX_RETRY_ATTEMPTS)
                                .build();
  }

  private ManagedChannel buildSecureChannel(final InstanceDetails remoteHost) {
    try {
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient().keyManager(certFile, pkeyFile);
      if (trustedCasFile != null) {
        sslContextBuilder.trustManager(trustedCasFile);
      }
      return NettyChannelBuilder.forAddress(remoteHost.getInstanceIp().toString(),
          getPortFromHost(remoteHost))
                                .sslContext(sslContextBuilder.build())
                                .enableRetry()
                                .maxRetryAttempts(MAX_RETRY_ATTEMPTS)
                                .build();
    } catch (SSLException e) {
      LOG.error("Unable to build an SSL gRPC client.", e);

      // Wrap the SSL Exception in a generic RTE and re-throw.
      throw new RuntimeException(e);
    }
  }

  private InterNodeRpcServiceStub buildStubForHost(final InstanceDetails remoteHost) {
    return InterNodeRpcServiceGrpc.newStub(getChannelForHost(remoteHost));
  }

  private void removeAllStubs() {
    for (Map.Entry<InstanceDetails.Id, AtomicReference<InterNodeRpcServiceStub>> entry : perHostClientStubMap.entrySet()) {
      LOG.debug("Removing client stub for host: {}", entry.getKey());
      perHostClientStubMap.remove(entry.getKey());
    }
  }

  private void terminateAllConnections() {
    for (Map.Entry<InstanceDetails.Id, AtomicReference<ManagedChannel>> entry : perHostChannelMap.entrySet()) {
      LOG.debug("shutting down connection to host: {}", entry.getKey());
      ManagedChannel channel = entry.getValue().get();
      channel.shutdownNow();
      try {
        channel.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.warn("Channel interrupted while shutting down", e);
        channel.shutdownNow();
        Thread.currentThread().interrupt();
      }

      perHostChannelMap.remove(entry.getKey());
    }
  }

  /**
   * Removes the stub and the channel object for the host.
   *
   * @param remoteHost the host to which we want to terminate connection from.
   */
  public void terminateConnection(InstanceDetails.Id remoteHost) {
    perHostClientStubMap.remove(remoteHost);
    perHostChannelMap.remove(remoteHost);
  }
}
