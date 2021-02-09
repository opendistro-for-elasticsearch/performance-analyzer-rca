/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CertificateUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Objects;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SubscriptionManagerTest {
    private GRPCConnectionManager grpcConnectionManager;
    private SubscriptionManager uut;

    private String oldCertificateFile;
    private String oldPrivateKeyFile;

    @Before
    public void setup() {
        oldCertificateFile = PluginSettings.instance().getProperty(CertificateUtils.CERTIFICATE_FILE_PATH);
        oldPrivateKeyFile = PluginSettings.instance().getProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH);

        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.CERTIFICATE_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/server/localhost.crt")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/server/localhost.key")).getFile());

        grpcConnectionManager = new GRPCConnectionManager(true);
        uut = new SubscriptionManager(grpcConnectionManager);
    }

    @After
    public void tearDown() {
        if (oldCertificateFile != null) {
            PluginSettings.instance().overrideProperty(CertificateUtils.CERTIFICATE_FILE_PATH, oldCertificateFile);
        }

        if (oldPrivateKeyFile != null) {
            PluginSettings.instance().overrideProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH, oldPrivateKeyFile);
        }
    }

    @Test
    public void testAddAndGetPublishers() {
        String testNode = "testNode";
        String ip1 = "127.0.0.1";
        InstanceDetails.Id id1 = new InstanceDetails.Id("id-1");
        InstanceDetails.Id id2 = new InstanceDetails.Id("id-2");
        String ip2 = "127.0.0.2";
        Assert.assertEquals(Collections.emptySet(), uut.getPublishersForNode(testNode));
        uut.addPublisher(testNode, id1);
        Assert.assertEquals(Sets.newHashSet(id1), uut.getPublishersForNode(testNode));
        uut.addPublisher(testNode, id2);
        Assert.assertEquals(Sets.newHashSet(id1, id2), uut.getPublishersForNode(testNode));
    }

    @Test
    public void testSubscriptionFlow() {
        String testNode = "testNode";
        InstanceDetails.Id id1 = new InstanceDetails.Id("id-1");
        InstanceDetails.Id id2 = new InstanceDetails.Id("id-2");
        String locus = "data-node";

        // Test that addSubscriber doesn't work on non-matching loci
        SubscribeResponse.SubscriptionStatus status = uut.addSubscriber(testNode, id1, locus);
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.TAG_MISMATCH, status);
        Assert.assertFalse(uut.isNodeSubscribed(testNode));

        // Test that addSubscriber works for matching loci
        uut.setCurrentLocus(locus);
        status = uut.addSubscriber(testNode, id1, locus);
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.SUCCESS, status);
        Assert.assertEquals(Sets.newHashSet(id1), uut.getSubscribersFor(testNode));
        Assert.assertTrue(uut.isNodeSubscribed(testNode));

        // Test that addSubscriber works on repeated calls
        status = uut.addSubscriber(testNode, id2, locus);
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.SUCCESS, status);
        Assert.assertEquals(Sets.newHashSet(id1, id2), uut.getSubscribersFor(testNode));
        Assert.assertTrue(uut.isNodeSubscribed(testNode));

        // Add host connections to the grpcConnectionManager
        grpcConnectionManager.getClientStubForHost(new InstanceDetails(id1,
                new InstanceDetails.Ip("0.0.0.0"), 9000));
        Assert.assertTrue(grpcConnectionManager.getPerHostChannelMap().containsKey(id1));
        Assert.assertTrue(grpcConnectionManager.getPerHostClientStubMap().containsKey(id1));
        grpcConnectionManager.getClientStubForHost(new InstanceDetails(id2, new InstanceDetails.Ip("0.0.0.0"), 9000));
        Assert.assertTrue(grpcConnectionManager.getPerHostChannelMap().containsKey(id2));
        Assert.assertTrue(grpcConnectionManager.getPerHostClientStubMap().containsKey(id2));

        // Test that unsubscribeAndTerminateConnection always terminates a connection
        // TODO is this actually the behavior we intended?
        uut.unsubscribeAndTerminateConnection("nonExistentNode", id1);
        Assert.assertFalse(grpcConnectionManager.getPerHostChannelMap().containsKey(id1));
        Assert.assertFalse(grpcConnectionManager.getPerHostClientStubMap().containsKey(id1));

        // Test that unsubscribeAndTerminateConnection properly updates the underlying map
        grpcConnectionManager.getClientStubForHost(new InstanceDetails(id2, new InstanceDetails.Ip("0.0.0.0"), 9000));
        uut.unsubscribeAndTerminateConnection(testNode, id2);
        Assert.assertEquals(Sets.newHashSet(id1), uut.getSubscribersFor(testNode));
        Assert.assertTrue(uut.isNodeSubscribed(testNode));
        Assert.assertFalse(grpcConnectionManager.getPerHostChannelMap().containsKey(id2));
        Assert.assertFalse(grpcConnectionManager.getPerHostClientStubMap().containsKey(id2));

        // Test that unsubscribeAndTerminateConnection doesn't update the underlying map for non existent addresses
        uut.unsubscribeAndTerminateConnection(testNode, new InstanceDetails.Id("nonExistentAddress"));
        Assert.assertEquals(Sets.newHashSet(id1), uut.getSubscribersFor(testNode));
        Assert.assertTrue(uut.isNodeSubscribed(testNode));

        // Test that unsubscribeAndTerminateConnection removes the node from the map once all of its subscriptions are
        // terminated
        uut.unsubscribeAndTerminateConnection(testNode, id1);
        Assert.assertEquals(Collections.emptySet(), uut.getSubscribersFor(testNode));
        Assert.assertFalse(uut.isNodeSubscribed(testNode));
    }
}
