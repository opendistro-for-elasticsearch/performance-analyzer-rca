package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SubscribeResponseHandlerTest {
    private static final String HOST = "127.0.0.1";
    private static final String NODE = "TEST";

    private SubscriptionManager subscriptionManager;
    private NodeStateManager nodeStateManager;
    private SubscribeResponseHandler uut;

    @Before
    public void setup() {
        GRPCConnectionManager grpcConnectionManager = new GRPCConnectionManager(true);
        subscriptionManager = new SubscriptionManager(grpcConnectionManager);
        nodeStateManager = new NodeStateManager();
        uut = new SubscribeResponseHandler(subscriptionManager, nodeStateManager, HOST, NODE);
    }

    @Test
    public void testOnNext() {
        // Test that onNext() properly processes a successful subscription message
        SubscribeResponse success = SubscribeResponse.newBuilder()
                .setSubscriptionStatus(SubscribeResponse.SubscriptionStatus.SUCCESS).build();
        uut.onNext(success);
        Assert.assertEquals(subscriptionManager.getPublishersForNode(NODE), Sets.newHashSet(HOST));
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.SUCCESS,
                nodeStateManager.getSubscriptionStatus(NODE, HOST));

        // Test that onNext() properly processes a tag mismatch subscription message
        SubscribeResponse mismatch = SubscribeResponse.newBuilder()
                .setSubscriptionStatus(SubscribeResponse.SubscriptionStatus.TAG_MISMATCH).build();
        uut.onNext(mismatch);
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.TAG_MISMATCH,
                nodeStateManager.getSubscriptionStatus(NODE, HOST));
        SubscribeResponse unknown = SubscribeResponse.newBuilder().build();
        uut.onNext(unknown); // This line is included for branch coverage
    }

    @Test
    public void testOnError() {
        /* No-op */
        uut.onError(new Exception("no-op"));
    }

    @Test
    public void testOnCompleted() {
        /* No-op */
        uut.onCompleted();
    }
}
