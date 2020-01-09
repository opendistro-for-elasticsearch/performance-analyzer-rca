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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse.PublishResponseStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.NetPersistor;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Service handler for the /sendData RPC. */
public class PublishRequestHandler {
  private static final Logger LOG = LogManager.getLogger(PublishRequestHandler.class);
  private final NetPersistor persistor;
  private final NodeStateManager nodeStateManager;
  private List<StreamObserver<PublishResponse>> upstreamResponseStreamList =
      Collections.synchronizedList(new ArrayList<>());

  public PublishRequestHandler(
      final NetPersistor persistor, final NodeStateManager nodeStateManager) {
    this.persistor = persistor;
    this.nodeStateManager = nodeStateManager;
  }

  public StreamObserver<FlowUnitMessage> getClientStream(
      final StreamObserver<PublishResponse> serviceResponse) {
    upstreamResponseStreamList.add(serviceResponse);
    return new SendDataClientStreamUpdateConsumer(serviceResponse);
  }

  public void terminateUpstreamConnections() {
    for (final StreamObserver<PublishResponse> responseStream : upstreamResponseStreamList) {
      responseStream.onNext(
          PublishResponse.newBuilder().setDataStatus(PublishResponseStatus.NODE_SHUTDOWN).build());
      responseStream.onCompleted();
    }
  }

  private class SendDataClientStreamUpdateConsumer implements StreamObserver<FlowUnitMessage> {
    private final StreamObserver<PublishResponse> serviceResponse;

    SendDataClientStreamUpdateConsumer(final StreamObserver<PublishResponse> serviceResponse) {
      this.serviceResponse = serviceResponse;
    }

    /**
     * Persist the flow unit sent by the client.
     *
     * @param flowUnitMessage The flow unit that the client just streamed to the server.
     */
    @Override
    public void onNext(FlowUnitMessage flowUnitMessage) {
      final String host = flowUnitMessage.getEsNode();
      final String graphNode = flowUnitMessage.getGraphNode();
      LOG.debug("Received flow unit from: {} for {}", host, graphNode);
      persistor.write(graphNode, flowUnitMessage);
      nodeStateManager.updateReceiveTime(host, graphNode);
    }

    /**
     * Client ran into an error while streaming FlowUnits.
     *
     * @param throwable The exception/error that the client encountered.
     */
    @Override
    public void onError(Throwable throwable) {
      LOG.error("Client ran into an error while streaming flow units: {}", throwable.getMessage());
      throwable.printStackTrace();
    }

    @Override
    public void onCompleted() {
      LOG.debug("Client finished streaming flow units");
      serviceResponse.onNext(buildDataResponse(PublishResponseStatus.SUCCESS));
      serviceResponse.onCompleted();
    }

    private PublishResponse buildDataResponse(final PublishResponseStatus status) {
      return PublishResponse.newBuilder().setDataStatus(status).build();
    }
  }
}
