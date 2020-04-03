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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.requests;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import io.grpc.stub.StreamObserver;

/**
 * Composite object that encapsulates the subscribe request message and the response stream.
 */
public class CompositeSubscribeRequest {

  /**
   * The subscribe protobuf message.
   */
  private final SubscribeMessage subscribeMessage;

  /**
   * The response stream to talk to the client on.
   */
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
