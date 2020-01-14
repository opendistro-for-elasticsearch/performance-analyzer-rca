package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import io.grpc.stub.StreamObserver;

public class CompositeSubscribeRequest {

  private final SubscribeMessage subscribeMessage;

  private final StreamObserver<SubscribeResponse> subscribeResponseStream;

  public CompositeSubscribeRequest(
      SubscribeMessage subscribeMessage,
      StreamObserver<SubscribeResponse> subscribeResponseStream) {
    this.subscribeMessage = subscribeMessage;
    this.subscribeResponseStream = subscribeResponseStream;
  }

  public SubscribeMessage getSubscribeMessage() {
    return subscribeMessage;
  }

  public StreamObserver<SubscribeResponse> getSubscribeResponseStream() {
    return subscribeResponseStream;
  }
}
