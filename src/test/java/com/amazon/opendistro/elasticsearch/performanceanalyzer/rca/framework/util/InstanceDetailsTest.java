package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import org.junit.Assert;
import org.junit.Test;

public class InstanceDetailsTest {
    @Test
    public void equality() {
        AllMetrics.NodeRole nodeRole = AllMetrics.NodeRole.MASTER;
        InstanceDetails.Id id = new InstanceDetails.Id("test-id");
        InstanceDetails.Id id2 = new InstanceDetails.Id("test-id");

        InstanceDetails.Ip ip = new InstanceDetails.Ip("127.0.0.1");
        InstanceDetails.Ip ip2 = new InstanceDetails.Ip("127.0.0.1");
        boolean isMaster = true;
        int grpcPort = 123;

        InstanceDetails instanceDetails1 = new InstanceDetails(nodeRole, id, ip, isMaster, grpcPort);
        ClusterDetailsEventProcessor.NodeDetails nodeDetails =
                new ClusterDetailsEventProcessor.NodeDetails(nodeRole, id2.toString(), ip2.toString(), isMaster, grpcPort);
        InstanceDetails instanceDetails2 = new InstanceDetails(nodeDetails);

        Assert.assertEquals(instanceDetails1, instanceDetails2);
        Assert.assertEquals(id.toString() + "::" + ip.toString() + "::" + nodeRole + "::" + grpcPort,
                instanceDetails1.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidIp() {
        InstanceDetails.Ip ip = new InstanceDetails.Ip("500.500.1.1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidId() {
        InstanceDetails.Id ip = new InstanceDetails.Id("127.0.0.1");
    }
}