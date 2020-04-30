package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.google.common.collect.Sets;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;


public class SubscriptionManagerTest {
    private GRPCConnectionManager grpcConnectionManager;
    private SubscriptionManager uut;

    @Before
    public void setup() {
        grpcConnectionManager = new GRPCConnectionManager(true);
        uut = new SubscriptionManager(grpcConnectionManager);
    }

    @Test
    public void testAddAndGetPublishers() {
        String testNode = "testNode";
        String ip1 = "127.0.0.1";
        String ip2 = "127.0.0.2";
        Assert.assertEquals(Collections.emptySet(), uut.getPublishersForNode(testNode));
        uut.addPublisher(testNode, ip1);
        Assert.assertEquals(Sets.newHashSet(ip1), uut.getPublishersForNode(testNode));
        uut.addPublisher(testNode, ip2);
        Assert.assertEquals(Sets.newHashSet(ip1, ip2), uut.getPublishersForNode(testNode));
    }

    @Test
    public void testSubscriptionFlow() {
        String testNode = "testNode";
        String ip1 = "127.0.0.1";
        String ip2 = "127.0.0.2";
        String locus = "data-node";

        // Test that addSubscriber doesn't work on non-matching loci
        SubscribeResponse.SubscriptionStatus status = uut.addSubscriber(testNode, ip1, locus);
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.TAG_MISMATCH, status);
        Assert.assertFalse(uut.isNodeSubscribed(testNode));

        // Test that addSubscriber works for matching loci
        uut.setCurrentLocus(locus);
        status = uut.addSubscriber(testNode, ip1, locus);
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.SUCCESS, status);
        Assert.assertEquals(Sets.newHashSet(ip1), uut.getSubscribersFor(testNode));
        Assert.assertTrue(uut.isNodeSubscribed(testNode));

        // Test that addSubscriber works on repeated calls
        status = uut.addSubscriber(testNode, ip2, locus);
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.SUCCESS, status);
        Assert.assertEquals(Sets.newHashSet(ip1, ip2), uut.getSubscribersFor(testNode));
        Assert.assertTrue(uut.isNodeSubscribed(testNode));

        // Add host connections to the grpcConnectionManager
        grpcConnectionManager.getClientStubForHost(ip1);
        Assert.assertTrue(grpcConnectionManager.getPerHostChannelMap().containsKey(ip1));
        Assert.assertTrue(grpcConnectionManager.getPerHostClientStubMap().containsKey(ip1));
        grpcConnectionManager.getClientStubForHost(ip2);
        Assert.assertTrue(grpcConnectionManager.getPerHostChannelMap().containsKey(ip2));
        Assert.assertTrue(grpcConnectionManager.getPerHostClientStubMap().containsKey(ip2));

        // Test that unsubscribeAndTerminateConnection always terminates a connection
        // TODO is this actually the behavior we intended?
        uut.unsubscribeAndTerminateConnection("nonExistentNode", ip1);
        Assert.assertFalse(grpcConnectionManager.getPerHostChannelMap().containsKey(ip1));
        Assert.assertFalse(grpcConnectionManager.getPerHostClientStubMap().containsKey(ip1));

        // Test that unsubscribeAndTerminateConnection properly updates the underlying map
        grpcConnectionManager.getClientStubForHost(ip2);
        uut.unsubscribeAndTerminateConnection(testNode, ip2);
        Assert.assertEquals(Sets.newHashSet(ip1), uut.getSubscribersFor(testNode));
        Assert.assertTrue(uut.isNodeSubscribed(testNode));
        Assert.assertFalse(grpcConnectionManager.getPerHostChannelMap().containsKey(ip2));
        Assert.assertFalse(grpcConnectionManager.getPerHostClientStubMap().containsKey(ip2));

        // Test that unsubscribeAndTerminateConnection doesn't update the underlying map for non existent addresses
        uut.unsubscribeAndTerminateConnection(testNode, "nonExistentAddress");
        Assert.assertEquals(Sets.newHashSet(ip1), uut.getSubscribersFor(testNode));
        Assert.assertTrue(uut.isNodeSubscribed(testNode));

        // Test that unsubscribeAndTerminateConnection removes the node from the map once all of its subscriptions are
        // terminated
        uut.unsubscribeAndTerminateConnection(testNode, ip1);
        Assert.assertEquals(Collections.emptySet(), uut.getSubscribersFor(testNode));
        Assert.assertFalse(uut.isNodeSubscribed(testNode));
    }
}
