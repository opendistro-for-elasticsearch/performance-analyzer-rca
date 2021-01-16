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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.PublishRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.SubscribeServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        try {
            netServer1.shutdown();
            netServer2.shutdown();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
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

        return new WireHopper(nodeStateManager, netClient, subscriptionManager, clientExecutor, receivedFlowUnitStore,
                appContext);
    }

    @Before
    public void setUp() {
        wireHopper1 = setUpWireHopper("instance1", port1, "instance2", port2);
        netServer1.setSendDataHandler(new PublishRequestHandler(
                wireHopper1.getNodeStateManager(),
                wireHopper1.getReceivedFlowUnitStore(),
                wireHopper1.getExecutorReference()));
        netServer1.setSubscribeHandler(
                new SubscribeServerHandler(
                        wireHopper1.getSubscriptionManager(),
                        wireHopper1.getExecutorReference()));

        wireHopper2 = setUpWireHopper("instance2", port2, "instance1", port1);
        netServer2.setSendDataHandler(new PublishRequestHandler(
                wireHopper2.getNodeStateManager(),
                wireHopper2.getReceivedFlowUnitStore(),
                wireHopper2.getExecutorReference()));
        netServer2.setSubscribeHandler(
                new SubscribeServerHandler(
                        wireHopper2.getSubscriptionManager(),
                        wireHopper2.getExecutorReference()));
    }

    /**
     * This test tries to create multiple NetServers on the same host, each listening on a different port.
     */
    @Test
    public void multipleNetServers() throws Exception {
        String gNode1 = "graphNode1";
        String gNode2 = "graphNode2";

        Map<String, String> rcaConfTags = new HashMap<>();
        rcaConfTags.put("locus", RcaConsts.RcaTagConstants.LOCUS_DATA_NODE);
        IntentMsg msg = new IntentMsg(gNode1, gNode2, rcaConfTags);
        wireHopper2.getSubscriptionManager().setCurrentLocus(RcaConsts.RcaTagConstants.LOCUS_DATA_NODE);

        wireHopper1.sendIntent(msg);

        WaitFor.waitFor(() ->
                        wireHopper2.getSubscriptionManager().getSubscribersFor(gNode2).size() == 1,
                10,
                TimeUnit.SECONDS);
        GenericFlowUnit flowUnit = new SymptomFlowUnit(System.currentTimeMillis());
        DataMsg dmsg = new DataMsg(gNode2, Lists.newArrayList(gNode1), Collections.singletonList(flowUnit));
        wireHopper2.sendData(dmsg);
        wireHopper1.getSubscriptionManager().setCurrentLocus(RcaConsts.RcaTagConstants.LOCUS_DATA_NODE);

        WaitFor.waitFor(() -> {
            List<FlowUnitMessage> receivedMags = wireHopper1.getReceivedFlowUnitStore().drainNode(gNode2);
            return receivedMags.size() == 1;
        }, 10, TimeUnit.SECONDS);
    }
}
