package com.opendestro.kafkaAdapter.util;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class UtilTest {

    private Target target1;
    private Target target2;


    @Before
    public void targetSetup (){
        target1 = new Target(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT,"ClusterTemperatureRca");
        target2 = new Target(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT);
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

    @Test
    public void testJsonToString() throws Exception{
        String testContext = "{\"sample key\": \"sample value\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(testContext);
        Assert.assertNotNull(Helper.convertJsonNodeToString(node));
    }

    @Test
    public void testHttpConnection() throws Exception {
        class UrlWrapper {
            URL url;
            public UrlWrapper(String spec) throws MalformedURLException {
                url = new URL(spec);
            }
            public HttpURLConnection openConnection() throws IOException {
                return (HttpURLConnection) url.openConnection();
            }
        }
        String testContext = "{\"sample key\": \"sample value\"}";
        String urlStr = "http://www.example.com";
        UrlWrapper u = PowerMockito.mock(UrlWrapper.class);
        Target target = Mockito.mock(Target.class);
        Mockito.when(target.getUrl()).thenReturn(urlStr);
        PowerMockito.whenNew(URL.class).withArguments(urlStr).thenReturn(u.url);
        HttpURLConnection con = PowerMockito.mock(HttpURLConnection.class);
        PowerMockito.when(u.openConnection()).thenReturn(con);
        PowerMockito.when(con.getResponseCode()).thenReturn(200);
        Assert.assertNotNull(Helper.makeRequest(target));

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(testContext);
        Assert.assertTrue(Helper.postToSlackWebHook(jsonNode,urlStr));
    }

}