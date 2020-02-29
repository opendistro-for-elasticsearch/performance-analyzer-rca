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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.SQLiteReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class ResourceHeatMapGraphTest {

    class AnalysisGraphTest extends DummyGraph {
        @Override
        public void construct() {
            super.constructResourceHeatMapGraph();
        }
    }

    static class RcaSchedulerTaskT extends RCASchedulerTask {
        static final int THREADS = 3;
        static String cwd = System.getProperty("user.dir");
        static Path sqliteFile = Paths.get(cwd, "src", "test", "resources", "metricsdbs",
                "metricsdb_1582865425000");
        static Queryable reader;

        static RcaConf rcaConf =
                new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
        static Persistable persistable;
        static final GRPCConnectionManager connectionManager = new GRPCConnectionManager(false);
        static final ClientServers clientServers =
                PerformanceAnalyzerApp.createClientServers(connectionManager);

        static SubscriptionManager subscriptionManager = new SubscriptionManager(connectionManager);
        static AtomicReference<ExecutorService> networkThreadPoolReference = new AtomicReference<>();

        static {
            try {
                persistable = PersistenceFactory.create(rcaConf);
            } catch (MalformedConfig malformedConfig) {
                malformedConfig.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                reader = new SQLiteReader(sqliteFile.toString());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

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
    }
}