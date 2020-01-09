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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.InterNodeRpcServiceGrpc;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class aims to abstract out managing client connections to the server and other boilerplate
 * stuff.
 */
public class NetClient {
  private static final Logger LOG = LogManager.getLogger(NetClient.class);

  private final GRPCConnectionManager connectionManager;

  public NetClient(final GRPCConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  private Map<String, StreamObserver<FlowUnitMessage>> perHostOpenDataStreamMap = new HashMap<>();

  public void subscribe(
      final String remoteHost,
      final SubscribeMessage subscribeMessage,
      StreamObserver<SubscribeResponse> serverResponseStream) {
    LOG.debug("Trying to send intent message to {}", remoteHost);
    connectionManager
        .getClientStubForHost(remoteHost)
        .subscribe(subscribeMessage, serverResponseStream);
  }

  public void publish(
      final String remoteHost,
      final FlowUnitMessage flowUnitMessage,
      final StreamObserver<PublishResponse> serverResponseStream) {
    LOG.debug("Publishing {} data to {}", flowUnitMessage.getGraphNode(), remoteHost);
    final StreamObserver<FlowUnitMessage> stream =
        getDataStreamForHost(remoteHost, serverResponseStream);
    stream.onNext(flowUnitMessage);
  }

  public void getMetrics(
      String remoteNodeIP,
      MetricsRequest request,
      StreamObserver<MetricsResponse> responseObserver) {
    InterNodeRpcServiceGrpc.InterNodeRpcServiceStub stub =
        connectionManager.getClientStubForHost(remoteNodeIP);
    stub.getMetrics(request, responseObserver);
  }

  public void shutdown() {
    LOG.debug("Shutting down client streaming connections..");
    closeAllDataStreams();
  }

  public void flushStream(final String remoteHost) {
    LOG.debug("removing data streams for {} as we are no publishing to it.", remoteHost);
    perHostOpenDataStreamMap.remove(remoteHost);
  }

  private void closeAllDataStreams() {
    for (Map.Entry<String, StreamObserver<FlowUnitMessage>> entry :
        perHostOpenDataStreamMap.entrySet()) {
      LOG.debug("Closing stream for host: {}", entry.getKey());
      // Sending an onCompleted should trigger the subscriber's node state manager
      // and cause this host to be put under observation.f
      entry.getValue().onCompleted();
      perHostOpenDataStreamMap.remove(entry.getKey());
    }
  }

  private StreamObserver<FlowUnitMessage> getDataStreamForHost(
      final String remoteHost, final StreamObserver<PublishResponse> serverResponseStream) {
    if (perHostOpenDataStreamMap.containsKey(remoteHost)) {
      return perHostOpenDataStreamMap.get(remoteHost);
    }

    InterNodeRpcServiceGrpc.InterNodeRpcServiceStub stub =
        connectionManager.getClientStubForHost(remoteHost);
    final StreamObserver<FlowUnitMessage> dataStream = stub.publish(serverResponseStream);

    perHostOpenDataStreamMap.put(remoteHost, dataStream);
    return dataStream;
  }

  public void dumpStreamStats() {
    LOG.debug("Active streams: {}", perHostOpenDataStreamMap);
  }
}
