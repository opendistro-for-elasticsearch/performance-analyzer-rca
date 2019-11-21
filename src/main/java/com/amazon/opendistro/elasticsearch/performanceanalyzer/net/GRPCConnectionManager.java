package com.amazon.opendistro.elasticsearch.performanceanalyzer.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.InterNodeRpcServiceGrpc;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterLevelMetricsReader;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  public List<String> getAllRemoteHosts() {
    final ClusterLevelMetricsReader.NodeDetails[] nodes = ClusterLevelMetricsReader.getNodes();
    final List<String> remoteHosts = new ArrayList<>();

    if (nodes != null && nodes.length > 1) {
      for (int i = 1; i < nodes.length; ++i) {
        remoteHosts.add(nodes[i].getHostAddress());
      }
    }

    return remoteHosts;
  }

  public String getCurrentHostAddress() {
    final ClusterLevelMetricsReader.NodeDetails[] nodes = ClusterLevelMetricsReader.getNodes();
    if (nodes.length > 0) {
      return nodes[0].getHostAddress();
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
    return ManagedChannelBuilder.forAddress(remoteHost, 9600).usePlaintext().build();
  }

  private ManagedChannel buildSecureChannel(final String remoteHost) {
    try {
      return NettyChannelBuilder.forAddress(remoteHost, 9600)
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
