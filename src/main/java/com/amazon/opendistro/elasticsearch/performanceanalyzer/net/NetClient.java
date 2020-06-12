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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.InterNodeRpcServiceGrpc;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.MetricsResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class aims to abstract out managing client connections to the server and other boilerplate
 * stuff.
 */
public class NetClient {

  private static final Logger LOG = LogManager.getLogger(NetClient.class);

  /**
   * The connection manager instance that holds objects needed to make RPCs.
   */
  private final GRPCConnectionManager connectionManager;

  public NetClient(final GRPCConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  private ConcurrentMap<String, AtomicReference<StreamObserver<FlowUnitMessage>>> perHostOpenDataStreamMap =
      new ConcurrentHashMap<>();

  /**
   * Sends a subscribe request to a remote host. If the subscribe request fails because the remote
   * host is not ready/encountered an exception, we still retry subscribing when we try reading from
   * remote hosts during graph execution.
   *
   * @param remoteHost           The host that the subscribe request is for.
   * @param subscribeMessage     The subscribe protobuf message.
   * @param serverResponseStream The response stream for the server to communicate back on.
   */
  public void subscribe(
      final String remoteHost,
      final SubscribeMessage subscribeMessage,
      StreamObserver<SubscribeResponse> serverResponseStream) {
    LOG.debug("Trying to send intent message to {}", remoteHost);
    try {
      connectionManager
          .getClientStubForHost(remoteHost)
          .subscribe(subscribeMessage, serverResponseStream);
      PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR
          .updateStat(RcaGraphMetrics.NET_BYTES_OUT, subscribeMessage.getRequesterNode(),
              subscribeMessage.getSerializedSize());
    } catch (StatusRuntimeException sre) {
      LOG.error("Encountered an error trying to subscribe. Status: {}",
          sre.getStatus(), sre);
      StatsCollector.instance().logException(StatExceptionCode.RCA_NETWORK_ERROR);
    }
  }

  /**
   * Gets a stream from the remote host to write flow units to. If there are failures while writing
   * to the stream, the subscribers will fail and trigger a new subscription which re-establishes
   * the stream.
   *
   * @param remoteHost           The remote host to which we need to send flow units to.
   * @param flowUnitMessage      The flow unit to send to the remote host.
   * @param serverResponseStream The stream for the server to communicate back on.
   */
  public void publish(
      final String remoteHost,
      final FlowUnitMessage flowUnitMessage,
      final StreamObserver<PublishResponse> serverResponseStream) {
    LOG.debug("Publishing {} data to {}", flowUnitMessage.getGraphNode(), remoteHost);
    try {
      final StreamObserver<FlowUnitMessage> stream =
          getDataStreamForHost(remoteHost, serverResponseStream);
      stream.onNext(flowUnitMessage);
      PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR
          .updateStat(RcaGraphMetrics.NET_BYTES_OUT, flowUnitMessage.getGraphNode(),
              flowUnitMessage.getSerializedSize());
    } catch (StatusRuntimeException sre) {
      LOG.error("rca: Encountered an error trying to publish a flow unit. Status: {}",
          sre.getStatus(), sre);
      StatsCollector.instance().logException(StatExceptionCode.RCA_NETWORK_ERROR);
    }
  }

  public void getMetrics(
      String remoteNodeIP,
      MetricsRequest request,
      StreamObserver<MetricsResponse> responseObserver) {
    InterNodeRpcServiceGrpc.InterNodeRpcServiceStub stub =
        connectionManager.getClientStubForHost(remoteNodeIP);
    stub.getMetrics(request, responseObserver);
  }

  public void stop() {
    LOG.debug("Shutting down client streaming connections..");
    closeAllDataStreams();
  }

  public void flushStream(final String remoteHost) {
    LOG.debug("removing data streams for {} as we are no publishing to it.", remoteHost);
    perHostOpenDataStreamMap.remove(remoteHost);
  }

  private void closeAllDataStreams() {
    for (Map.Entry<String, AtomicReference<StreamObserver<FlowUnitMessage>>> entry :
        perHostOpenDataStreamMap.entrySet()) {
      LOG.debug("Closing stream for host: {}", entry.getKey());
      // Sending an onCompleted should trigger the subscriber's node state manager
      // and cause this host to be put under observation.f
      entry.getValue().get().onCompleted();
      perHostOpenDataStreamMap.remove(entry.getKey());
    }
  }

  private StreamObserver<FlowUnitMessage> getDataStreamForHost(
      final String remoteHost, final StreamObserver<PublishResponse> serverResponseStream) {
    final AtomicReference<StreamObserver<FlowUnitMessage>> streamObserverAtomicReference =
        perHostOpenDataStreamMap.get(remoteHost);
    if (streamObserverAtomicReference != null) {
      return streamObserverAtomicReference.get();
    }
    return addOrUpdateDataStreamForHost(remoteHost, serverResponseStream);
  }

  /**
   * Builds or updates a flow unit data stream to a host. Callers: Send data thread.
   *
   * @param remoteHost           The host to which we want to open a stream to.
   * @param serverResponseStream The response stream object.
   * @return A stream to the host.
   */
  private synchronized StreamObserver<FlowUnitMessage> addOrUpdateDataStreamForHost(
      final String remoteHost, final StreamObserver<PublishResponse> serverResponseStream) {
    InterNodeRpcServiceGrpc.InterNodeRpcServiceStub stub =
        connectionManager.getClientStubForHost(remoteHost);
    final StreamObserver<FlowUnitMessage> dataStream = stub.publish(serverResponseStream);
    perHostOpenDataStreamMap.computeIfAbsent(remoteHost, s -> new AtomicReference<>());
    perHostOpenDataStreamMap.get(remoteHost).set(dataStream);
    return dataStream;
  }
}
