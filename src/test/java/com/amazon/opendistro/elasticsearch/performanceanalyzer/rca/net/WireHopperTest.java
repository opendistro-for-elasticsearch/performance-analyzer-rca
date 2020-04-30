package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.PublishRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.SubscribeServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class WireHopperTest {
    // TestNetServer is a NetServer that clients can check is running or not
    private static class TestNetServer extends NetServer implements Runnable {
        public AtomicBoolean isRunning = new AtomicBoolean(false);

        public TestNetServer(final int port, final int numServerThreads, final boolean useHttps) {
            super(port, numServerThreads, useHttps);
        }

        @Override
        protected void postStartHook() {
            isRunning.set(true);
        }

        @Override
        protected void shutdownHook() {
            isRunning.set(false);
        }
    }

    private static final String NODE1 = "NODE1";
    private static final String NODE2 = "NODE2";
    private static final String LOCALHOST = "127.0.0.1";
    private static final String HOST_NOT_IN_CLUSTER = "NOTINCLUSTER";
    private static final String LOCUS = "data-node";
    private static final long EVAL_INTERVAL_S = 5L;
    private static final long TIMESTAMP = 66L;
    private static final ExecutorService rejectingExecutor = new RejectingExecutor();

    private static NetClient netClient;
    private static TestNetServer netServer;
    private static ExecutorService executorService;
    private static ExecutorService netServerExecutor;
    private static AtomicReference<ExecutorService> clientExecutor;
    private static AtomicReference<ExecutorService> serverExecutor;
    private static GRPCConnectionManager connectionManager;

    private SubscriptionManager subscriptionManager;
    private NodeStateManager nodeStateManager;
    private ReceivedFlowUnitStore receivedFlowUnitStore;
    private WireHopper uut; // Unit under test

    @BeforeClass
    public static void setupClass() throws Exception {
        connectionManager = new GRPCConnectionManager(PluginSettings.instance().getHttpsEnabled());
        netClient = new NetClient(connectionManager);
        executorService = Executors.newSingleThreadExecutor();
        clientExecutor = new AtomicReference<>(null);
        serverExecutor = new AtomicReference<>(Executors.newSingleThreadExecutor());
        netServer = new TestNetServer(Util.RPC_PORT, 1, false);
        netServerExecutor = Executors.newSingleThreadExecutor();
        netServerExecutor.execute(netServer);
        // Wait for the TestNetServer to start
        WaitFor.waitFor(() -> netServer.isRunning.get(), 10, TimeUnit.SECONDS);
        if (!netServer.isRunning.get()) {
            throw new RuntimeException("Unable to start TestNetServer");
        }
    }

    @Before
    public void setup() {
        nodeStateManager = new NodeStateManager();
        receivedFlowUnitStore = new ReceivedFlowUnitStore();
        subscriptionManager = new SubscriptionManager(connectionManager);
        clientExecutor.set(null);
        uut = new WireHopper(nodeStateManager, netClient, subscriptionManager, clientExecutor, receivedFlowUnitStore);
    }

    @AfterClass
    public static void tearDown() {
        executorService.shutdown();
        netServerExecutor.shutdown();
        netServer.stop();
        netClient.stop();
    }

    @Test
    public void testSendIntent() throws Exception {
        Node<MetricFlowUnit> node = new Heap_Used(EVAL_INTERVAL_S);
        netServer.setSubscribeHandler(new SubscribeServerHandler(subscriptionManager, serverExecutor));
        Map<String, String> rcaConfTags = new HashMap<>();
        rcaConfTags.put("locus", RcaConsts.RcaTagConstants.LOCUS_DATA_NODE);
        IntentMsg msg = new IntentMsg(NODE1, node.name(), rcaConfTags);
        // verify resilience to null executor
        uut.sendIntent(msg);
        // verify method generates appropriate task
        clientExecutor.set(executorService);
        subscriptionManager.setCurrentLocus(RcaConsts.RcaTagConstants.LOCUS_DATA_NODE);
        ClusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(NODE1, LOCALHOST, false),
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(node.name(), LOCALHOST, false)
        ));
        uut.sendIntent(msg);
        WaitFor.waitFor(() -> subscriptionManager.getSubscribersFor(node.name()).size() == 1, 1,
                TimeUnit.SECONDS);
        Assert.assertEquals(1, subscriptionManager.getSubscribersFor(node.name()).size());
        Assert.assertEquals(LOCALHOST, subscriptionManager.getSubscribersFor(node.name()).asList().get(0));
        // verify resilience to RejectedExecutionException
        clientExecutor.set(rejectingExecutor);
        uut.sendIntent(msg);
    }

    @Test
    public void testSendData() throws Exception {
        netServer.setSendDataHandler(new PublishRequestHandler(nodeStateManager, receivedFlowUnitStore, serverExecutor));
        // verify resilience to null executor
        GenericFlowUnit flowUnit = new SymptomFlowUnit(TIMESTAMP);
        DataMsg msg = new DataMsg(NODE1, Lists.newArrayList(NODE2), Collections.singletonList(flowUnit));
        uut.sendData(msg);

        clientExecutor.set(executorService);
        Assert.assertEquals(0L, nodeStateManager.getLastReceivedTimestamp(NODE1, LOCALHOST));
        // setup downstream subscribers
        subscriptionManager.setCurrentLocus(LOCUS);
        subscriptionManager.addSubscriber(NODE1, LOCALHOST, LOCUS);
        // verify sendData works
        ClusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(NODE1, LOCALHOST, false),
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(NODE2, LOCALHOST, false)
        ));
        uut.sendData(msg);
        WaitFor.waitFor(() -> nodeStateManager.getLastReceivedTimestamp(NODE1, LOCALHOST) != 0, 1,
                TimeUnit.SECONDS);
        // Verify that the data gets persisted into receivedFlowUnitStore once it's received
        WaitFor.waitFor(() -> {
            List<FlowUnitMessage> receivedMags = receivedFlowUnitStore.drainNode(NODE1);
            return receivedMags.size() == 1;
        }, 10, TimeUnit.SECONDS);
        // verify resilience to RejectedExecutionException
        clientExecutor.set(rejectingExecutor);
        uut.sendData(msg);
    }

    @Test
    public void testReadFromWire() throws Exception {
        netServer.setSubscribeHandler(new SubscribeServerHandler(subscriptionManager, serverExecutor));
        // Setup mock object responses
        Node<MetricFlowUnit> node = new Heap_Used(EVAL_INTERVAL_S);
        node.addTag(RcaConsts.RcaTagConstants.TAG_LOCUS, RcaConsts.RcaTagConstants.LOCUS_DATA_MASTER_NODE);
        // Verify resilience to null executor
        uut.readFromWire(node);
        // Execute test method and verify return value
        clientExecutor.set(executorService);
        ClusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(
                        node.name(), LOCALHOST, false)));
        subscriptionManager.setCurrentLocus(RcaConsts.RcaTagConstants.LOCUS_DATA_NODE);
        subscriptionManager.addPublisher(node.name(), LOCALHOST);
        subscriptionManager.addPublisher(node.name(), HOST_NOT_IN_CLUSTER);
        nodeStateManager.updateReceiveTime(LOCALHOST, node.name(), 1L);
        FlowUnitMessage msg = FlowUnitMessage.newBuilder().setGraphNode(node.name()).build();
        ImmutableList<FlowUnitMessage> msgList = ImmutableList.<FlowUnitMessage>builder().add(msg).build();
        receivedFlowUnitStore.enqueue(node.name(), msg);
        List<FlowUnitMessage> actualMsgList = uut.readFromWire(node);
        Assert.assertEquals(msgList, actualMsgList);
        // Verify expected interactions with the subscription manager
        WaitFor.waitFor(() -> subscriptionManager.getSubscribersFor(node.name()).size() == 1, 1,
                TimeUnit.SECONDS);
        Assert.assertEquals(LOCALHOST, subscriptionManager.getSubscribersFor(node.name()).asList().get(0));
        // Verify resilience to RejectedExecutionException
        clientExecutor.set(rejectingExecutor);
        uut.readFromWire(node);
    }
}
