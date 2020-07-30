package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterUtilsTest {
    private static final String HOST1 = "host1";
    private static final String HOST2 = "host2";
    private static final ClusterDetailsEventProcessor.NodeDetails EMPTY_DETAILS =
            ClusterDetailsEventProcessorTestHelper.newNodeDetails("", "", false);
    private ClusterDetailsEventProcessor clusterDetailsEventProcessor;

    private List<InstanceDetails> getInstancesFromHost(List<String> hostIps) {
        List<InstanceDetails> instances = new ArrayList<>();
        for (String id: hostIps) {
            InstanceDetails instance = new InstanceDetails(AllMetrics.NodeRole.UNKNOWN, new InstanceDetails.Id(id),
                    new InstanceDetails.Ip("0.0.0.0"), false);
            instances.add(instance);
        }
        return instances;
    }

    @Before
    public void setup() {
        clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
        clusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(EMPTY_DETAILS));
    }

    @Test
    public void testIsHostAddressInCluster() {
        // method should return false when there are no peers
        Assert.assertFalse(ClusterUtils.isHostIdInCluster(new InstanceDetails.Id(HOST1), getInstancesFromHost(Collections.EMPTY_LIST)));
        // method should properly recognize which hosts are peers and which aren't
        clusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(null, HOST1, false)
        ));



        List<InstanceDetails> instances = getInstancesFromHost(Collections.singletonList(HOST1));

        Assert.assertTrue(ClusterUtils.isHostIdInCluster(new InstanceDetails.Id(HOST1), instances));
        Assert.assertFalse(ClusterUtils.isHostIdInCluster(new InstanceDetails.Id(HOST2), instances));
    }
}
