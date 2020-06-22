package com.amazon.opendistro.elasticsearch.performanceanalyzer.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class GRPCTest {
    private static final Logger LOG = LogManager.getLogger(GRPCTest.class);

    private static NetClient netClient;
    private static NetClient insecureClient;
    private static TestNetServer netServer;
    private static ExecutorService executorService;
    private static ExecutorService netServerExecutor;
    private static AtomicReference<ExecutorService> clientExecutor;
    private static AtomicReference<ExecutorService> serverExecutor;
    private static GRPCConnectionManager connectionManager;
    private static GRPCConnectionManager insecureConnectionManager;

    @BeforeClass
    public static void setup() throws Exception {
        try {
            connectionManager = new GRPCConnectionManager(true);
            netClient = new NetClient(connectionManager);
            insecureConnectionManager = new GRPCConnectionManager(false);
            insecureClient = new NetClient(insecureConnectionManager);
            executorService = Executors.newSingleThreadExecutor();
            clientExecutor = new AtomicReference<>(null);
            serverExecutor = new AtomicReference<>(Executors.newSingleThreadExecutor());
            netServer = new TestNetServer(Util.RPC_PORT, 1, true);
            netServerExecutor = Executors.newSingleThreadExecutor();
            netServerExecutor.execute(netServer);
            // Wait for the TestNetServer to start
            WaitFor.waitFor(() -> netServer.isRunning.get(), 10, TimeUnit.SECONDS);
            if (!netServer.isRunning.get()) {
                throw new RuntimeException("Unable to start TestNetServer");
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize NetTest", e);
            throw e;
        }
    }

    @AfterClass
    public static void tearDown() {
        executorService.shutdown();
        netServerExecutor.shutdown();
        netServer.stop();
        netClient.stop();
        insecureClient.stop();
        connectionManager.shutdown();
        insecureConnectionManager.shutdown();
    }

    @Test
    public void testSecureGetMetrics() throws Exception {
        MetricsRequest request = MetricsRequest.newBuilder()
                .addMetricList("CPU_UTILIZATION")
                .addAggList("avg")
                .addDimList("ShardId")
                .build();
        final MetricsResponse[] response = new MetricsResponse[1];
        StreamObserver<MetricsResponse> observer = new StreamObserver<MetricsResponse>() {
            @Override
            public void onNext(MetricsResponse value) {
                LOG.info("onNext called!");
                response[0] = value;
            }

            @Override
            public void onError(Throwable t) {
                LOG.error("GetMetrics observer received error from server", t);
            }

            @Override
            public void onCompleted() {
                LOG.info("GetMetrics stream completed successfully");
            }
        };
        netClient.getMetrics("127.0.0.1", request, observer);
        WaitFor.waitFor(() -> {
            return response[0] != null && response[0].getMetricsResult().equals("metrics");
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Verifies that an unauthorized client should not be able to communicate with a TLS secured server
     */
    @Test
    public void testUnauthorizedGetMetricsFails() throws Exception {
        MetricsRequest request = MetricsRequest.newBuilder()
                .addMetricList("CPU_UTILIZATION")
                .addAggList("avg")
                .addDimList("ShardId")
                .build();
        final Throwable[] errors = new Throwable[1];
        StreamObserver<MetricsResponse> observer = new StreamObserver<MetricsResponse>() {
            @Override
            public void onNext(MetricsResponse value) {
                LOG.error("onNext called successfully with insecure connection");
            }

            @Override
            public void onError(Throwable t) {
                errors[0] = t;
            }

            @Override
            public void onCompleted() {
                LOG.info("GetMetrics stream completed successfully");
            }
        };

        try {
            insecureClient.getMetrics("localhost", request, observer);
        } catch (RuntimeException e) {
            return;
        }
        WaitFor.waitFor(() -> {
            if (errors[0] != null) {
                if (errors[0] instanceof StatusRuntimeException) {
                    return true;
                }
                throw new Exception("Wanted StatusRuntimeException, but got unexpected error: {}", errors[0]);
            }
            return false;
        }, 30, TimeUnit.SECONDS);
    }
}
