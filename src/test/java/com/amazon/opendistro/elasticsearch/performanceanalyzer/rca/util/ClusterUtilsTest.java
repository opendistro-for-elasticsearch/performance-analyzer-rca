package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import com.google.common.collect.Lists;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterUtilsTest {
    private static final String HOST = "127.0.0.1";
    private static final String HOST2 = "127.0.0.2";
    private static final ClusterDetailsEventProcessor.NodeDetails EMPTY_DETAILS =
            ClusterDetailsEventProcessorTestHelper.newNodeDetails("", "", false);

    @Before
    public void setup() {
        ClusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(EMPTY_DETAILS));
    }

    @Test
    public void testGetCurrentHostAddress() {
        Assert.assertEquals(ClusterUtils.EMPTY_STRING, ClusterUtils.getCurrentNodeHostAddress());
        ClusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(null, HOST, false)
        ));
        Assert.assertEquals(HOST, ClusterUtils.getCurrentNodeHostAddress());
    }

    @Test
    public void testGetAllPeerHostAddresses() {
        // method should behave when fed an empty list of peers
        Assert.assertEquals(Collections.emptyList(), ClusterUtils.getAllPeerHostAddresses());
        // method should not include the current node in the list of peers
        ClusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(null, HOST, false)
        ));
        Assert.assertEquals(Collections.emptyList(), ClusterUtils.getAllPeerHostAddresses());
        // method should return the appropriate peers when peers exist
        ClusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(null, HOST, false),
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(null, HOST2, false)
        ));
        Assert.assertEquals(Collections.singletonList(HOST2), ClusterUtils.getAllPeerHostAddresses());
    }

    @Test
    public void testIsHostAddressInCluster() {
        // method should return false when there are no peers
        Assert.assertFalse(ClusterUtils.isHostAddressInCluster(HOST));
        // method should properly recognize which hosts are peers and which aren't
        ClusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
                ClusterDetailsEventProcessorTestHelper.newNodeDetails(null, HOST, false)
        ));
        Assert.assertTrue(ClusterUtils.isHostAddressInCluster(HOST));
        Assert.assertFalse(ClusterUtils.isHostAddressInCluster(HOST2));
    }
}
