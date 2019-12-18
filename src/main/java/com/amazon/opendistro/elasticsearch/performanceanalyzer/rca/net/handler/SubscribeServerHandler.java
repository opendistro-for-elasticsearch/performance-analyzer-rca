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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SubscribeServerHandler {
  private static final Logger LOG = LogManager.getLogger(SubscribeServerHandler.class);
  private static final String EMPTY_STRING = "";
  private static final String REQUESTER_KEY = "requester";
  private static final String LOCUS_KEY = "locus";

  private final WireHopper hopper;
  private final SubscriptionManager subscriptionManager;

  public SubscribeServerHandler(
      final WireHopper hopper, final SubscriptionManager subscriptionManager) {
    this.hopper = hopper;
    this.subscriptionManager = subscriptionManager;
  }

  public void handleSubscriptionRequest(
      final SubscribeMessage request, final StreamObserver<SubscribeResponse> responseObserver) {
    LOG.debug(
        "Received intent from a downstream:{} for {}",
        request.getDestinationNode(),
        request.getRequesterNode());
    final Map<String, String> tags = request.getTagsMap();
    final String requesterHostAddress = tags.getOrDefault(REQUESTER_KEY, EMPTY_STRING);
    final String locus = tags.getOrDefault(LOCUS_KEY, EMPTY_STRING);
    //        final boolean subscriptionStatus =
    // hopper.addSubscription(request.getDestinationNode(), requesterHostAddress, locus);
    final SubscriptionStatus subscriptionStatus =
        subscriptionManager.addSubscriber(
            request.getDestinationNode(), requesterHostAddress, locus);

    responseObserver.onNext(
        SubscribeResponse.newBuilder().setSubscriptionStatus(subscriptionStatus).build());
    responseObserver.onCompleted();
  }
}
