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
    private static final String HOST = "127.0.0.1";
    private static final String HOST2 = "127.0.0.2";
    private static final ClusterDetailsEventProcessor.NodeDetails EMPTY_DETAILS =
            ClusterDetailsEventProcessorTestHelper.newNodeDetails("", "", false);
    private ClusterDetailsEventProcessor clusterDetailsEventProcessor;

    private List<InstanceDetails> getInstancesFromHost(List<String> hostIps) {
        List<InstanceDetails> instances = new ArrayList<>();
        for (String ip: hostIps) {
            InstanceDetails instance = new InstanceDetails(AllMetrics.NodeRole.UNKNOWN, "", ip, false);
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
        Assert.assertFalse(ClusterUtils.isHostAddressInCluster(HOST, getInstancesFromHost(Collections.EMPTY_LIST)));
        // method should properly recognize which hosts are peers and which aren't
        clusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(null, HOST, false)
        ));



        List<InstanceDetails> instances = getInstancesFromHost(Collections.singletonList(HOST));

        Assert.assertTrue(ClusterUtils.isHostAddressInCluster(HOST, instances));
        Assert.assertFalse(ClusterUtils.isHostAddressInCluster(HOST2, instances));
    }
}
