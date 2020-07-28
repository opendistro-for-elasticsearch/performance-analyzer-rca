package com.amazon.opendistro.elasticsearch.performanceanalyzer.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.PublishRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.SubscribeServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultipleNetServers {

    private static int port1 = 9800;
    private static int port2 = 9801;

    private static TestNetServer netServer1;
    private static TestNetServer netServer2;

    private static WireHopper wireHopper1;
    private static WireHopper wireHopper2;

    private static TestNetServer startServer(int port) throws Exception {
        TestNetServer netServer = new TestNetServer(port, 1, false);
        ExecutorService netServerExecutor = Executors.newSingleThreadExecutor();

        netServerExecutor.execute(netServer);
        // Wait for the TestNetServer to start
        WaitFor.waitFor(() -> netServer.isRunning.get(), 10, TimeUnit.SECONDS);
        if (!netServer.isRunning.get()) {
            throw new RuntimeException("Unable to start TestNetServer");
        }
        return netServer;
    }

    @BeforeClass
    public static void classSetUp() throws Exception {
        netServer1 = startServer(port1);
        netServer2 = startServer(port2);
    }

    @AfterClass
    public static void shutdown() {
        wireHopper1.shutdownAll();
        wireHopper2.shutdownAll();

        netServer1.shutdown();
        netServer2.shutdown();
    }

    private ClusterDetailsEventProcessor.NodeDetails createNodeDetails(int port, String instance) {
        return new ClusterDetailsEventProcessor.NodeDetails(
                AllMetrics.NodeRole.DATA, instance, "127.0.0.1", false, port);
    }

    private ClusterDetailsEventProcessor createClusterDetails(ClusterDetailsEventProcessor.NodeDetails node1,
                                                              ClusterDetailsEventProcessor.NodeDetails node2) {
        ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
        clusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(node1, node2));
        return clusterDetailsEventProcessor;
    }

    public WireHopper setUpWireHopper(String instance1, int port1, String instance2, int port2) {
        ClusterDetailsEventProcessor clusterDetailsEventProcessor = createClusterDetails(
                createNodeDetails(port1, instance1),
                createNodeDetails(port2, instance2)
        );

        AppContext appContext = new AppContext();
        appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

        GRPCConnectionManager connectionManager = new GRPCConnectionManager(false);
        NodeStateManager nodeStateManager = new NodeStateManager(appContext);
        AtomicReference<ExecutorService> clientExecutor = new AtomicReference<>(Executors.newSingleThreadExecutor());
        NetClient netClient = new NetClient(connectionManager);
        SubscriptionManager subscriptionManager = new SubscriptionManager(connectionManager);
        ReceivedFlowUnitStore receivedFlowUnitStore = new ReceivedFlowUnitStore();

        // netServer1.setSendDataHandler(new PublishRequestHandler(
        //         nodeStateManager, receivedFlowUnitStore, networkThreadPoolReference));
        // netServer.setSubscribeHandler(
        //         new SubscribeServerHandler(subscriptionManager, networkThreadPoolReference));


        return new WireHopper(nodeStateManager, netClient, subscriptionManager, clientExecutor, receivedFlowUnitStore,
                appContext);
    }

    @Before
    public void setUp() {
        wireHopper1 = setUpWireHopper("instance1", port1, "instance2", port2);
        wireHopper2 = setUpWireHopper("instance2", port2, "instance1", port1);
    }

    /**
     * This test tries to create multiple NetServers on the same host, each listening on a different port.
     */
    @Test
    public void multipleNetServers() {
        wireHopper1.sendIntent(
                new IntentMsg(
                        "node1", "node2", ImmutableMap.of("locus", "instance1")));

        System.out.println("Tests are done !!");

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
