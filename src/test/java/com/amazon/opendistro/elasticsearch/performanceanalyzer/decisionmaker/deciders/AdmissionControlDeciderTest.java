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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.HEAP_MAX_SIZE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.AdmissionControlClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.core.util.Assert;
import org.junit.Before;
import org.junit.Test;

public class AdmissionControlDeciderTest {

    AppContext appContext;
    RcaConf rcaConf;

    @Before
    public void setupCluster() {
        ClusterDetailsEventProcessor clusterDetailsEventProcessor =
                new ClusterDetailsEventProcessor();

        ClusterDetailsEventProcessor.NodeDetails node1 =
                new ClusterDetailsEventProcessor.NodeDetails(
                        AllMetrics.NodeRole.DATA, "node1", "127.0.0.1", false);
        ClusterDetailsEventProcessor.NodeDetails node2 =
                new ClusterDetailsEventProcessor.NodeDetails(
                        AllMetrics.NodeRole.DATA, "node2", "127.0.0.2", false);
        ClusterDetailsEventProcessor.NodeDetails node3 =
                new ClusterDetailsEventProcessor.NodeDetails(
                        AllMetrics.NodeRole.DATA, "node3", "127.0.0.3", false);
        ClusterDetailsEventProcessor.NodeDetails node4 =
                new ClusterDetailsEventProcessor.NodeDetails(
                        AllMetrics.NodeRole.DATA, "node3", "127.0.0.4", false);
        ClusterDetailsEventProcessor.NodeDetails master =
                new ClusterDetailsEventProcessor.NodeDetails(
                        AllMetrics.NodeRole.ELECTED_MASTER, "master", "127.0.0.9", true);

        List<ClusterDetailsEventProcessor.NodeDetails> nodes = new ArrayList<>();
        nodes.add(node1);
        nodes.add(node2);
        nodes.add(node3);
        nodes.add(node4);
        nodes.add(master);
        clusterDetailsEventProcessor.setNodesDetails(nodes);

        appContext = new AppContext();
        appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

        String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString();
        rcaConf = new RcaConf(rcaConfPath);
    }

    @Test
    public void testAdmissionControlDecider() {
        RcaTestHelper<HotNodeSummary> admissionControlNodeRca =
                new RcaTestHelper<>("AdmissionControlNodeRca");
        admissionControlNodeRca.setAppContext(this.appContext);

        AdmissionControlClusterRca admissionControlClusterRca =
                new AdmissionControlClusterRca(1, admissionControlNodeRca);
        admissionControlClusterRca.setAppContext(this.appContext);

        AdmissionControlDecider admissionControlDecider =
                new AdmissionControlDecider(1, 1, admissionControlClusterRca);
        admissionControlDecider.setAppContext(this.appContext);
        admissionControlDecider.readRcaConf(this.rcaConf);

        ResourceContext resourceContext = new ResourceContext(Resources.State.UNHEALTHY);
        NodeKey nodeKey =
                new NodeKey(new InstanceDetails.Id("node1"), new InstanceDetails.Ip("127.0.0.1"));
        HotResourceSummary resourceSummary = new HotResourceSummary(HEAP_MAX_SIZE, 1.0, 1.0, 0);
        HotNodeSummary nodeSummary =
                new HotNodeSummary(nodeKey.getNodeId(), nodeKey.getHostAddress());
        nodeSummary.appendNestedSummary(resourceSummary);
        HotClusterSummary clusterSummary = new HotClusterSummary(5, 1);
        clusterSummary.appendNestedSummary(nodeSummary);

        admissionControlClusterRca.setLocalFlowUnit(
                new ResourceFlowUnit<>(
                        System.currentTimeMillis(), resourceContext, clusterSummary, true));

        Decision decision = admissionControlDecider.operate();
        Assert.isNonEmpty(decision);
    }
}
