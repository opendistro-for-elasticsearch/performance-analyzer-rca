/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterDimensionalSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactClusterLevelNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistenceFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCASchedulerTask;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryRcaRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.SQLiteReader;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ResourceHeatMapGraphTest {
    private final int THREADS = 3;
    private final String cwd = System.getProperty("user.dir");
    private final Path sqliteFile =
            Paths.get(cwd, "src", "test", "resources", "metricsdbs", "metricsdb_1582865425000");

    private final RcaConf rcaConf =
            new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());

    private Queryable reader;
    private Persistable persistable;
    private GRPCConnectionManager connectionManager;
    private ClientServers clientServers;

    private SubscriptionManager subscriptionManager;
    private AtomicReference<ExecutorService> networkThreadPoolReference;

    @Before
    public void init() {
        try {
            persistable = PersistenceFactory.create(rcaConf);
        } catch (MalformedConfig malformedConfig) {
            malformedConfig.printStackTrace();
            Assert.fail();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            reader = new SQLiteReader(sqliteFile.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }

        AllMetrics.NodeRole nodeRole2 = AllMetrics.NodeRole.ELECTED_MASTER;
        RcaTestHelper.setMyIp("192.168.0.2", nodeRole2);
        connectionManager = new GRPCConnectionManager(false);
        clientServers = PerformanceAnalyzerApp.createClientServers(connectionManager);

        HttpServer httpServer = clientServers.getHttpServer();
        httpServer.start();

        QueryRcaRequestHandler rcaRequestHandler = new QueryRcaRequestHandler();
        rcaRequestHandler.setPersistable(persistable);
        httpServer.createContext(Util.RCA_QUERY_URL, rcaRequestHandler);

        subscriptionManager = new SubscriptionManager(connectionManager);
        networkThreadPoolReference = new AtomicReference<>();
    }

    @After
    public void shutdown() {
        connectionManager.shutdown();
        clientServers.getHttpServer().stop(0);
        clientServers.getNetServer().stop();
        clientServers.getNetClient().stop();
    }

    class AnalysisGraphTest extends DummyGraph {
        @Override
        public void construct() {
            super.constructResourceHeatMapGraph();
        }
    }

    class RcaSchedulerTaskT extends RCASchedulerTask {

        public RcaSchedulerTaskT(List<ConnectedComponent> connectedComponents, RcaConf rcaConf,
                                 SubscriptionManager subscriptionManager) {
            super(
                    1000,
                    Executors.newFixedThreadPool(THREADS),
                    connectedComponents,
                    reader,
                    persistable,
                    rcaConf,
                    new WireHopper(new NodeStateManager(), clientServers.getNetClient(),
                            subscriptionManager,
                            networkThreadPoolReference,
                            new ReceivedFlowUnitStore(rcaConf.getPerVertexBufferLength())));
        }
    }

    @Test
    public void constructResourceHeatMapGraph() {
        AnalysisGraph analysisGraph = new ResourceHeatMapGraphTest.AnalysisGraphTest();
        List<ConnectedComponent> connectedComponents =
                RcaUtil.getAnalysisGraphComponents(analysisGraph);
        RcaTestHelper.setEvaluationTimeForAllNodes(connectedComponents, 1);

        String dataNodeRcaConf = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString();

        RcaConf rcaConf = new RcaConf(dataNodeRcaConf);
        SubscriptionManager subscriptionManager =
                new SubscriptionManager(new GRPCConnectionManager(false));
        subscriptionManager.setCurrentLocus(rcaConf.getTagMap().get("locus"));

        RCASchedulerTask rcaSchedulerTaskData =
                new ResourceHeatMapGraphTest.RcaSchedulerTaskT(connectedComponents, rcaConf,
                        subscriptionManager);
        AllMetrics.NodeRole nodeRole = AllMetrics.NodeRole.DATA;
        RcaTestHelper.setMyIp("192.168.0.1", nodeRole);
        rcaSchedulerTaskData.run();

        System.out.println("Now for the MAster RCA.");
        String masterNodeRcaConf =
                Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_elected_master.conf").toString();
        RcaConf rcaConf2 = new RcaConf(masterNodeRcaConf);
        SubscriptionManager subscriptionManager2 =
                new SubscriptionManager(new GRPCConnectionManager(false));
        subscriptionManager2.setCurrentLocus(rcaConf2.getTagMap().get("locus"));
        RCASchedulerTask rcaSchedulerTaskMaster =
                new ResourceHeatMapGraphTest.RcaSchedulerTaskT(connectedComponents, rcaConf2,
                        subscriptionManager2);
        AllMetrics.NodeRole nodeRole2 = AllMetrics.NodeRole.ELECTED_MASTER;
        RcaTestHelper.setMyIp("192.168.0.2", nodeRole2);
        rcaSchedulerTaskMaster.run();

        URL url = null;
        try {
            url = new URL("http://localhost:9600" + Util.RCA_QUERY_URL);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.addRequestProperty("name", ClusterTemperatureRca.TABLE_NAME);
            con.setRequestMethod("GET");

            int status = con.getResponseCode();
            System.out.println("Response status: " + status);
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()))) {
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }

                testJsonResponse(content.toString());
            }

            con.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    /**
     * {
     * "ClusterTemperatureRca": [
     * {
     * "rca_name": "ClusterTemperatureRca",
     * "timestamp": 1586975949087,
     * "state": "unknown",
     * "ClusterTemperatureSummary": [
     * {
     * "ClusterDimensionalTemperatureSummary": [
     * {
     * "dimension": "CPU_Utilization",
     * "mean": 10,
     * "total": 1.20827386264977,
     * "numNodes": 1,
     * "ClusterZoneProfileSummary": [
     * {
     * "zone": "HOT",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * },
     * {
     * "zone": "WARM",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * },
     * {
     * "zone": "LUKE_WARM",
     * "min": null,
     * "max": null,
     * "all_nodes": [
     * {
     * "host_address": "192.168.0.1",
     * "node_id": "4sqG_APMQuaQwEW17_6zwg"
     * }
     * ]
     * },
     * {
     * "zone": "COLD",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * }
     * ]
     * },
     * {
     * "dimension": "Heap_AllocRate",
     * "mean": 0,
     * "total": 0,
     * "numNodes": 1,
     * "ClusterZoneProfileSummary": [
     * {
     * "zone": "HOT",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * },
     * {
     * "zone": "WARM",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * },
     * {
     * "zone": "LUKE_WARM",
     * "min": null,
     * "max": null,
     * "all_nodes": [
     * {
     * "host_address": "192.168.0.1",
     * "node_id": "4sqG_APMQuaQwEW17_6zwg"
     * }
     * ]
     * },
     * {
     * "zone": "COLD",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * }
     * ]
     * },
     * {
     * "dimension": "IO_READ_SYSCALL_RATE",
     * "mean": 0,
     * "total": 0,
     * "numNodes": 1,
     * "ClusterZoneProfileSummary": [
     * {
     * "zone": "HOT",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * },
     * {
     * "zone": "WARM",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * },
     * {
     * "zone": "LUKE_WARM",
     * "min": null,
     * "max": null,
     * "all_nodes": [
     * {
     * "host_address": "192.168.0.1",
     * "node_id": "4sqG_APMQuaQwEW17_6zwg"
     * }
     * ]
     * },
     * {
     * "zone": "COLD",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * }
     * ]
     * },
     * {
     * "dimension": "IO_WriteSyscallRate",
     * "mean": 0,
     * "total": 0,
     * "numNodes": 1,
     * "ClusterZoneProfileSummary": [
     * {
     * "zone": "HOT",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * },
     * {
     * "zone": "WARM",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * },
     * {
     * "zone": "LUKE_WARM",
     * "min": null,
     * "max": null,
     * "all_nodes": [
     * {
     * "host_address": "192.168.0.1",
     * "node_id": "4sqG_APMQuaQwEW17_6zwg"
     * }
     * ]
     * },
     * {
     * "zone": "COLD",
     * "min": null,
     * "max": null,
     * "all_nodes": []
     * }
     * ]
     * }
     * ],
     * "CompactClusterLevelNodeSummary": [
     * {
     * "node_id": "4sqG_APMQuaQwEW17_6zwg",
     * "host_address": "192.168.0.1",
     * "CPU_Utilization_mean": 10,
     * "CPU_Utilization_total": 1.20827386264977,
     * "CPU_Utilization_num_shards": 3,
     * "Heap_AllocRate_mean": 0,
     * "Heap_AllocRate_total": 0,
     * "Heap_AllocRate_num_shards": 0,
     * "IO_READ_SYSCALL_RATE_mean": 0,
     * "IO_READ_SYSCALL_RATE_total": 0,
     * "IO_READ_SYSCALL_RATE_num_shards": 0,
     * "IO_WriteSyscallRate_mean": 0,
     * "IO_WriteSyscallRate_total": 0,
     * "IO_WriteSyscallRate_num_shards": 0
     * }
     * ]
     * }
     * ]
     * }
     * ],
     * "HighHeapUsageClusterRca": []
     * }
     *
     */
    private void testJsonResponse(String jsonResponse) {
        final String clusterTempRca = ClusterTemperatureRca.TABLE_NAME;
        final String clusterTempSummaryStr = ClusterTemperatureSummary.TABLE_NAME;
        final String clusterDimSummary = ClusterDimensionalSummary.TABLE_NAME;
        final String clusterZoneSummary = ClusterDimensionalSummary.ZONE_PROFILE_TABLE_NAME;
        final String clusterLevelNodeSummary = CompactClusterLevelNodeSummary.class.getSimpleName();

        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(jsonResponse);
        JsonObject temperatureRca =
                jsonElement.getAsJsonObject().get(clusterTempRca).getAsJsonArray().get(0).getAsJsonObject();

        Assert.assertEquals(clusterTempRca, temperatureRca.get("rca_name").getAsString());
        Assert.assertEquals("unknown", temperatureRca.get("state").getAsString());

        JsonObject clusterTempSummary =
                temperatureRca.get(clusterTempSummaryStr).getAsJsonArray().get(0).getAsJsonObject();

        JsonArray dimensionArray =
                clusterTempSummary.get(clusterDimSummary).getAsJsonArray();
        for (int i = 0; i < dimensionArray.size(); i++) {
            JsonObject dimensionObj = dimensionArray.get(i).getAsJsonObject();
            if (dimensionObj.get("dimension").getAsString().equals("CPU_Utilization")) {
                int mean = dimensionObj.get("mean").getAsInt();
                Assert.assertEquals(10, mean);

                double total = dimensionObj.get("total").getAsDouble();
                Assert.assertEquals(1.208273862649, total, 0.01);


                int numNodes = dimensionObj.get("numNodes").getAsInt();
                Assert.assertEquals(1, numNodes);

                JsonArray zoneArr = dimensionObj.get(clusterZoneSummary).getAsJsonArray();
                for (int j = 0; j < dimensionArray.size(); j++) {
                    JsonObject zoneObject = zoneArr.get(j).getAsJsonObject();
                    if (zoneObject.get("zone").getAsString().equals("LUKE_WARM")) {
                        Assert.assertTrue(zoneObject.get("min").isJsonNull());
                        Assert.assertTrue(zoneObject.get("max").isJsonNull());

                        JsonArray allNodesArr = zoneObject.get("all_nodes").getAsJsonArray();

                        for (int k = 0; k < allNodesArr.size(); k++) {
                            JsonObject nodeObj = allNodesArr.get(k).getAsJsonObject();
                            Assert.assertEquals("192.168.0.1",
                                    nodeObj.get("host_address").getAsString());
                            Assert.assertEquals("4sqG_APMQuaQwEW17_6zwg",
                                    nodeObj.get("node_id").getAsString());
                        }
                    }
                }

            }
        }

        JsonArray nodeDetailsArr =
                clusterTempSummary.get(clusterLevelNodeSummary).getAsJsonArray();
        for (int i = 0; i < nodeDetailsArr.size(); i++) {
            JsonObject node = nodeDetailsArr.get(i).getAsJsonObject();
            // "node_id": "4sqG_APMQuaQwEW17_6zwg",
            // "host_address": "192.168.0.1",
            // "CPU_Utilization_mean": 10,
            // "CPU_Utilization_total": 1.20827386264977,
            // "CPU_Utilization_num_shards": 3,
            // "Heap_AllocRate_mean": 0,
            // "Heap_AllocRate_total": 0,
            // "Heap_AllocRate_num_shards": 0,
            // "IO_READ_SYSCALL_RATE_mean": 0,
            // "IO_READ_SYSCALL_RATE_total": 0,
            // "IO_READ_SYSCALL_RATE_num_shards": 0,
            // "IO_WriteSyscallRate_mean": 0,
            // "IO_WriteSyscallRate_total": 0,
            // "IO_WriteSyscallRate_num_shards": 0

            Assert.assertEquals("4sqG_APMQuaQwEW17_6zwg", node.get("node_id").getAsString());
            Assert.assertEquals("192.168.0.1", node.get("host_address").getAsString());
            Assert.assertEquals(10, node.get("CPU_Utilization_mean").getAsInt());
            Assert.assertEquals(1.20827386264977, node.get("CPU_Utilization_total").getAsDouble(),
                    0.01);
            Assert.assertEquals(3, node.get("CPU_Utilization_num_shards").getAsInt());

            Assert.assertEquals(10, node.get("Heap_AllocRate_mean").getAsInt());
            Assert.assertEquals(239855498, node.get("Heap_AllocRate_total").getAsInt());
            Assert.assertEquals(3, node.get("Heap_AllocRate_num_shards").getAsInt());

            Assert.assertEquals(0, node.get("IO_READ_SYSCALL_RATE_mean").getAsInt());
            Assert.assertEquals(0, node.get("IO_READ_SYSCALL_RATE_total").getAsInt());
            Assert.assertEquals(0, node.get("IO_READ_SYSCALL_RATE_num_shards").getAsInt());

            Assert.assertEquals(0, node.get("IO_WriteSyscallRate_mean").getAsInt());
            Assert.assertEquals(0, node.get("IO_WriteSyscallRate_total").getAsInt());
            Assert.assertEquals(0, node.get("IO_WriteSyscallRate_num_shards").getAsInt());
        }
    }
}