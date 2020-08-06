package com.opendestro.kafkaAdapter.util;
import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;

public class TargetTest {

    private Target target1;
    private Target target2;


    @Before
    public void targetSetup (){
        target1 = new Target(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT,"ClusterTemperatureRca");
        target2 = new Target(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT);
    }

    @Test
    public void getUrlTest() {
        Assert.assertEquals("http://localhost:9600/_opendistro/_performanceanalyzer/rca?name=ClusterTemperatureRca",target1.getUrl());
        Assert.assertEquals("http://localhost:9600/_opendistro/_performanceanalyzer/rca", target2.getUrl());
    }

    @Test
    public void endpointTest() {
        Assert.assertEquals(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT, target1.getEndpoint());
        target1.setEndpoint("localhost:9650");
        Assert.assertEquals("localhost:9650", target1.getEndpoint());
    }

    @Test
    public void parameterTest() {
        Assert.assertNull(target2.getParameter());
        Assert.assertEquals("ClusterTemperatureRca", target1.getParameter());
        target2.setParameter("HighHeapUsageClusterRca");
        Assert.assertEquals("HighHeapUsageClusterRca", target2.getParameter());
    }
}