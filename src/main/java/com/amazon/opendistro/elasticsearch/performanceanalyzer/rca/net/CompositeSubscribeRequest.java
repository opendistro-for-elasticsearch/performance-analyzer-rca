package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import io.grpc.stub.StreamObserver;

/**
 * Composite object that encapsulates the subscribe request message and the response stream.
 */
public class CompositeSubscribeRequest {

  private final SubscribeMessage subscribeMessage;

  private final StreamObserver<SubscribeResponse> subscribeResponseStream;

  public CompositeSubscribeRequest(
      SubscribeMessage subscribeMessage,
      StreamObserver<SubscribeResponse> subscribeResponseStream) {
    this.subscribeMessage = subscribeMessage;
    this.subscribeResponseStream = subscribeResponseStream;
  }

  /**
   * Get the subscribe request.
   * @return The subscribe request protobuf message.
   */
  public SubscribeMessage getSubscribeMessage() {
    return subscribeMessage;
  }

  /**
   * Get the response stream for the request returned by getSubscribeMessage().
   * @return The response stream to write response to for the subscribe request.
   */
  public StreamObserver<SubscribeResponse> getSubscribeResponseStream() {
    return subscribeResponseStream;
  }
}
