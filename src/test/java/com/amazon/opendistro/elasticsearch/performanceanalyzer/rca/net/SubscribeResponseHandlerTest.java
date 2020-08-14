package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.CertificateUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.google.common.collect.Sets;
import java.util.Objects;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SubscribeResponseHandlerTest {
    private static final InstanceDetails.Ip HOST = new InstanceDetails.Ip("127.0.0.1");
    private static final InstanceDetails.Id HOST_ID = new InstanceDetails.Id("host1");
    private static final String GRAPH_NODE = "TEST";

    private SubscriptionManager subscriptionManager;
    private NodeStateManager nodeStateManager;
    private SubscribeResponseHandler uut;

    private String oldCertificateFile;
    private String oldPrivateKeyFile;

    @Before
    public void setup() {
        ClassLoader classLoader = getClass().getClassLoader();

        oldCertificateFile = PluginSettings.instance().getProperty(CertificateUtils.CERTIFICATE_FILE_PATH);
        oldPrivateKeyFile = PluginSettings.instance().getProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH);

        PluginSettings.instance().overrideProperty(CertificateUtils.CERTIFICATE_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/server/localhost.crt")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/server/localhost.key")).getFile());

        GRPCConnectionManager grpcConnectionManager = new GRPCConnectionManager(true);
        subscriptionManager = new SubscriptionManager(grpcConnectionManager);
        nodeStateManager = new NodeStateManager(new AppContext());

        InstanceDetails remoteInstance = new InstanceDetails(HOST_ID, HOST, -1);
        uut = new SubscribeResponseHandler(subscriptionManager, nodeStateManager, remoteInstance, GRAPH_NODE);
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
    public void testOnNext() {
        // Test that onNext() properly processes a successful subscription message
        SubscribeResponse success = SubscribeResponse.newBuilder()
                .setSubscriptionStatus(SubscribeResponse.SubscriptionStatus.SUCCESS).build();
        uut.onNext(success);
        Assert.assertEquals(subscriptionManager.getPublishersForNode(GRAPH_NODE), Sets.newHashSet(HOST_ID));
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.SUCCESS,
                nodeStateManager.getSubscriptionStatus(GRAPH_NODE, HOST_ID));

        // Test that onNext() properly processes a tag mismatch subscription message
        SubscribeResponse mismatch = SubscribeResponse.newBuilder()
                .setSubscriptionStatus(SubscribeResponse.SubscriptionStatus.TAG_MISMATCH).build();
        uut.onNext(mismatch);
        Assert.assertEquals(SubscribeResponse.SubscriptionStatus.TAG_MISMATCH,
                nodeStateManager.getSubscriptionStatus(GRAPH_NODE, HOST_ID));
        SubscribeResponse unknown = SubscribeResponse.newBuilder().build();
        uut.onNext(unknown); // This line is included for branch coverage
    }

    @Test
    public void testOnCompleted() {
        /* No-op */
        uut.onCompleted();
    }
}
