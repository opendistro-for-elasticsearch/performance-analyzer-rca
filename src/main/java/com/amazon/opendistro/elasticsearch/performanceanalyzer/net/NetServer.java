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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CertificateUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.InterNodeRpcServiceGrpc;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.handler.MetricsServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.PublishRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.SubscribeServerHandler;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetServer extends InterNodeRpcServiceGrpc.InterNodeRpcServiceImplBase
    implements Runnable {

  private static final Logger LOG = LogManager.getLogger(NetServer.class);

  private final int port;
  private final int numServerThreads;
  private final boolean useHttps;

  private PublishRequestHandler sendDataHandler;
  private SubscribeServerHandler subscribeHandler;
  private MetricsServerHandler metricsServerHandler;

  private Server server;

  public NetServer(final int port, final int numServerThreads, final boolean useHttps) {
    this.port = port;
    this.numServerThreads = numServerThreads;
    this.useHttps = useHttps;
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used to create a thread,
   * starting the thread causes the object's <code>run</code> method to be called in that separately
   * executing thread.
   *
   * <p>The general contract of the method <code>run</code> is that it may take any action
   * whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    LOG.info(
        "Starting the gRPC server on port {} with {} threads. Using HTTPS: {}",
        port,
        numServerThreads,
        useHttps);

    server = useHttps ? buildHttpsServer() : buildHttpServer();
    try {
      server.start();
      LOG.info("gRPC server started successfully!");
      server.awaitTermination();
      LOG.info(" gRPC server terminating..");
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  private Server buildHttpServer() {
    return NettyServerBuilder.forPort(port)
                             .addService(this)
                             .bossEventLoopGroup(new NioEventLoopGroup(numServerThreads))
                             .workerEventLoopGroup(new NioEventLoopGroup(numServerThreads))
                             .channelType(NioServerSocketChannel.class)
                             .executor(Executors.newSingleThreadExecutor())
                             .build();
  }

  private Server buildHttpsServer() {
    return NettyServerBuilder.forPort(port)
                             .addService(this)
                             .bossEventLoopGroup(new NioEventLoopGroup(numServerThreads))
                             .workerEventLoopGroup(new NioEventLoopGroup(numServerThreads))
                             .channelType(NioServerSocketChannel.class)
                             .useTransportSecurity(
                                 CertificateUtils.getCertificateFile(),
                                 CertificateUtils.getPrivateKeyFile())
                             .build();
  }

  /**
   * <pre>
   * Sends a flowunit to whoever is interested in it.
   * </pre>
   *
   * @param responseObserver The response stream.
   */
  @Override
  public StreamObserver<FlowUnitMessage> publish(
      final StreamObserver<PublishResponse> responseObserver) {
    LOG.debug("publish received");
    if (sendDataHandler != null) {
      return sendDataHandler.getClientStream(responseObserver);
    }

    throw new UnsupportedOperationException("No rpc handler found for publish/");
  }

  /**
   * <pre>
   * Sends a subscription request to a node for a particular metric.
   * </pre>
   *
   * @param request          The subscribe request.
   * @param responseObserver The response stream to which subscription status is written to.
   */
  @Override
  public void subscribe(
      final SubscribeMessage request, final StreamObserver<SubscribeResponse> responseObserver) {
    LOG.debug("subscribe received");
    if (subscribeHandler != null) {
      subscribeHandler.handleSubscriptionRequest(request, responseObserver);
    }

    throw new UnsupportedOperationException("No rpc handler found for subscribe/");
  }

  @Override
  public void getMetrics(MetricsRequest request, StreamObserver<MetricsResponse> responseObserver) {
    if (metricsServerHandler != null) {
      metricsServerHandler.collectAPIData(request, responseObserver);
    }
  }

  public void setSubscribeHandler(SubscribeServerHandler subscribeHandler) {
    this.subscribeHandler = subscribeHandler;
  }

  public void setSendDataHandler(PublishRequestHandler sendDataHandler) {
    this.sendDataHandler = sendDataHandler;
  }

  public void setMetricsHandler(MetricsServerHandler metricsServerHandler) {
    this.metricsServerHandler = metricsServerHandler;
  }

  public void shutdown() {
    LOG.debug("indicating upstream nodes that current node is going down..");
    if (sendDataHandler != null) {
      sendDataHandler.terminateUpstreamConnections();
    }
    // server.shutdownNow();
  }
}
