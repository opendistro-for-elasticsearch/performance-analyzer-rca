package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class SubscribeServerHandler {
    private static final Logger LOG = LogManager.getLogger(SubscribeServerHandler.class);
    private static final String EMPTY_STRING = "";
    private static final String REQUESTER_KEY = "requester";
    private static final String LOCUS_KEY = "locus";

    private final WireHopper hopper;
    private final SubscriptionManager subscriptionManager;

    public SubscribeServerHandler(final WireHopper hopper, final SubscriptionManager subscriptionManager) {
        this.hopper = hopper;
        this.subscriptionManager = subscriptionManager;
    }

    public void handleSubscriptionRequest(final SubscribeMessage request, final StreamObserver<SubscribeResponse> responseObserver) {
        LOG.debug("Received intent from a downstream:{} for {}", request.getDestinationNode(), request.getRequesterNode());
        final Map<String, String> tags = request.getTagsMap();
        final String requesterHostAddress = tags.getOrDefault(REQUESTER_KEY, EMPTY_STRING);
        final String locus = tags.getOrDefault(LOCUS_KEY, EMPTY_STRING);
//        final boolean subscriptionStatus = hopper.addSubscription(request.getDestinationNode(), requesterHostAddress, locus);
        final SubscriptionStatus subscriptionStatus = subscriptionManager.addSubscriber(request.getDestinationNode(), requesterHostAddress, locus);

        responseObserver.onNext(SubscribeResponse.newBuilder()
                                                 .setSubscriptionStatus(subscriptionStatus)
                                                 .build());
        responseObserver.onCompleted();
    }
}
