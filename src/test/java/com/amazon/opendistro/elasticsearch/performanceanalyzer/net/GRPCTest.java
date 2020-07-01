package com.amazon.opendistro.elasticsearch.performanceanalyzer.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CertificateUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * GRPCTest tests that our gRPC servers and clients perform mutual authentication when TLS is enabled
 */
@Category(GradleTaskForRca.class)
public class GRPCTest {
    private static final Logger LOG = LogManager.getLogger(GRPCTest.class);
    private static final MetricsRequest METRICS_REQUEST = MetricsRequest.newBuilder()
            .addMetricList("CPU_UTILIZATION")
            .addAggList("avg")
            .addDimList("ShardId")
            .build();

    private static TestNetServer netServer;

    /**
     * Starts a TestNetServer then waits until the server is running
     * @param netServer The TestNetServer to start
     * @throws Exception If the function times out before the server starts running
     */
    public static void startTestNetServer(final TestNetServer netServer) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.execute(netServer);
            WaitFor.waitFor(() -> netServer.isRunning.get(), 10, TimeUnit.SECONDS);
            if (!netServer.isRunning.get()) {
                throw new RuntimeException("Unable to start TestNetServer");
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize TestNetServer", e);
            throw e;
        }
    }

    @BeforeClass
    // setupServer sets up the gRPC TestNetServer on port 9650
    public static void setupServer() throws Exception {
        netServer = new TestNetServer(Util.RPC_PORT, 1, true);
        startTestNetServer(netServer);
    }

    @AfterClass
    public static void tearDown() {
        netServer.shutdown();
    }

    /**
     * testSecureGetMetrics tests that a client and server can communicate properly when certificates are properly
     * configured
     */
    @Test
    public void testSecureGetMetrics() throws Exception {
        // Setup client with trusted identity
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_CERTIFICATE_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/client/localhost.crt")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_PRIVATE_KEY_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/client/localhost.key")).getFile());
        NetClient validClient = new NetClient(new GRPCConnectionManager(true));
        // Make getMetrics request to server
        ResponseObserver observer = new ResponseObserver();
        validClient.getMetrics("127.0.0.1", METRICS_REQUEST, observer);
        // Wait for the expected response from the server
        WaitFor.waitFor(() -> {
            return observer.responses[0] != null && observer.responses[0].getMetricsResult().equals("metrics");
        }, 15, TimeUnit.SECONDS);
        validClient.stop();
        validClient.getConnectionManager().shutdown();
    }

    /**
     * testInvalidGetMetricsFails tests that a non-TLS client can't communicate with a TLS secured server
     */
    @Test
    public void testInvalidGetMetricsFails() throws Exception {
        // Setup client with untrusted identity
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_CERTIFICATE_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/attacker/attack_cert.pem")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_PRIVATE_KEY_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/attacker/attack_key.pem")).getFile());
        NetClient invalidClient = new NetClient(new GRPCConnectionManager(true));
        // Make invalid getMetrics request to server
        ErrorObserver<MetricsResponse> observer = new ErrorObserver<>();
        invalidClient.getMetrics("127.0.0.1", METRICS_REQUEST, observer);
        // Wait for the client to fail
        WaitFor.waitFor(() -> observer.errors[0] != null && observer.errors[0] instanceof StatusRuntimeException,
                15, TimeUnit.SECONDS);
        invalidClient.stop();
        invalidClient.getConnectionManager().shutdown();

    }

    /**
     * testNonTLSGetMetricsFails tests that the server doesn't communicate with an unauthenticated client
     */
    @Test
    public void testNonTLSGetMetricsFails() throws Exception {
        NetClient insecureClient = new NetClient(new GRPCConnectionManager(false));
        // Make invalid getMetrics request to server
        ErrorObserver<MetricsResponse> observer = new ErrorObserver<>();
        insecureClient.getMetrics("127.0.0.1", METRICS_REQUEST, observer);
        // Wait for the client to fail
        WaitFor.waitFor(() -> observer.errors[0] != null && observer.errors[0] instanceof StatusRuntimeException,
                15, TimeUnit.SECONDS);
        insecureClient.stop();
        insecureClient.getConnectionManager().shutdown();
    }

    /**
     * testClientShouldRejectUntrustedServer tests that a client doesn't communicate with an unauthenticated server
     */
    @Test
    public void testClientShouldRejectUntrustedServer() throws Exception {
        // Setup a client which shouldn't trust our TestNetServer
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/rootca/root2ca.pem")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_CERTIFICATE_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/client/localhost.crt")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_PRIVATE_KEY_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/client/localhost.key")).getFile());
        NetClient client = new NetClient(new GRPCConnectionManager(true));
        // Make valid getMetrics request to server
        ErrorObserver<MetricsResponse> observer = new ErrorObserver<>();
        client.getMetrics("127.0.0.1", METRICS_REQUEST, observer);
        // Client should fail because the server cert isn't signed by the client CA
        WaitFor.waitFor(() -> observer.errors[0] != null && observer.errors[0] instanceof StatusRuntimeException,
                15, TimeUnit.SECONDS);
        client.stop();
        client.getConnectionManager().shutdown();
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH, "");
    }

    // ErrorObserver is a StreamObserver that stores the most recent error it receives
    private static class ErrorObserver<T> implements StreamObserver<T> {
        public Throwable[] errors = new Throwable[1];

        @Override
        public void onNext(T value) {
            LOG.error("ErrorObserver received a value from the server: {}", value);
        }

        @Override
        public void onError(Throwable t) {
            errors[0] = t;
        }

        @Override
        public void onCompleted() {
            LOG.info("ErrorObserver completed successfully");
        }
    }

    // ResponseObserver is a StreamObserver that stores the most recent response it receives
    private static class ResponseObserver implements StreamObserver<MetricsResponse> {
        MetricsResponse[] responses = new MetricsResponse[1];

        @Override
        public void onNext(MetricsResponse value) {
            responses[0] = value;
        }

        @Override
        public void onError(Throwable t) {
            LOG.error("ResponseObserver received error from server", t);
        }

        @Override
        public void onCompleted() {
            LOG.info("ResponseObserver completed successfully");
        }
    }
}
