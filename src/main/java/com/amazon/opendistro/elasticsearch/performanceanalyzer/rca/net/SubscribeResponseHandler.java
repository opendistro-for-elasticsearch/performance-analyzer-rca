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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.SubscribeResponse.SubscriptionStatus;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Stream observer class that handles the response to a subscription request.
 */
public class SubscribeResponseHandler implements StreamObserver<SubscribeResponse> {

  private static final Logger LOG = LogManager.getLogger(SubscribeResponseHandler.class);

  /**
   * The subscription manager instance to update subscription metadata.
   */
  private final SubscriptionManager subscriptionManager;

  /**
   * The node state manager instance to update tracking staleness.
   */
  private final NodeStateManager nodeStateManager;

  /**
   * The host address of the remote host.
   */
  private final String remoteHost;

  /**
   * The vertex that we want to subscribe to.
   */
  private final String graphNode;

  public SubscribeResponseHandler(final SubscriptionManager subscriptionManager,
      final NodeStateManager nodeStateManager,
      final String remoteHost, final String graphNode) {
    this.subscriptionManager = subscriptionManager;
    this.nodeStateManager = nodeStateManager;
    this.graphNode = graphNode;
    this.remoteHost = remoteHost;
  }

  /**
   * Receives a value from the stream.
   *
   * <p>Can be called many times but is never called after {@link #onError(Throwable)} or {@link
   * #onCompleted()} are called.
   *
   * <p>Unary calls must invoke onNext at most once.  Clients may invoke onNext at most once for
   * server streaming calls, but may receive many onNext callbacks.  Servers may invoke onNext at
   * most once for client streaming calls, but may receive many onNext callbacks.
   *
   * <p>If an exception is thrown by an implementation the caller is expected to terminate the
   * stream by calling {@link #onError(Throwable)} with the caught exception prior to propagating
   * it.
   *
   * @param subscribeResponse the value passed to the stream
   */
  @Override
  public void onNext(SubscribeResponse subscribeResponse) {
    if (subscribeResponse.getSubscriptionStatus() == SubscriptionStatus.SUCCESS) {
      subscriptionManager.addPublisher(graphNode, remoteHost);
      nodeStateManager.updateSubscriptionState(graphNode, remoteHost, SubscriptionStatus.SUCCESS);
    } else if (subscribeResponse.getSubscriptionStatus() == SubscriptionStatus.TAG_MISMATCH) {
      nodeStateManager
          .updateSubscriptionState(graphNode, remoteHost, SubscriptionStatus.TAG_MISMATCH);
    }
  }

  /**
   * Receives a terminating error from the stream.
   *
   * <p>May only be called once and if called it must be the last method called. In particular if
   * an
   * exception is thrown by an implementation of {@code onError} no further calls to any method are
   * allowed.
   *
   * <p>{@code t} should be a {@link StatusException} or {@link
   * StatusRuntimeException}, but other {@code Throwable} types are possible. Callers should
   * generally convert from a {@link Status} via {@link Status#asException()} or {@link
   * Status#asRuntimeException()}. Implementations should generally convert to a {@code Status} via
   * {@link Status#fromThrowable(Throwable)}.
   *
   * @param t the error occurred on the stream
   */
  @Override
  public void onError(Throwable t) {
    LOG.error("Encountered an error while processing subscription stream", t);
  }

  /**
   * Receives a notification of successful stream completion.
   *
   * <p>May only be called once and if called it must be the last method called. In particular if
   * an
   * exception is thrown by an implementation of {@code onCompleted} no further calls to any method
   * are allowed.
   */
  @Override
  public void onCompleted() {
    LOG.info("Finished subscription request for {}. Closing stream.", remoteHost);
  }
}
