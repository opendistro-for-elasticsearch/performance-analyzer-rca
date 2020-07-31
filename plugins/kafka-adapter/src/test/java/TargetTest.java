import com.harold.tool.Target;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TargetTest {

    private Target target1;
    private Target target2;


    @Before
    public void targetSetup (){
        target1 = new Target("localhost:9600/_opendistro/_performanceanalyzer/rca","ClusterTemperatureRca");
        target2 = new Target("localhost:9600/_opendistro/_performanceanalyzer/rca");
    }

    @Test
    public void getUrlTest() {
        assertEquals("http://localhost:9600/_opendistro/_performanceanalyzer/rca?name=ClusterTemperatureRca",target1.getUrl());
        assertEquals("http://localhost:9600/_opendistro/_performanceanalyzer/rca", target2.getUrl());
    }

    @Test
    public void endpointTest() {
        assertEquals("localhost:9600/_opendistro/_performanceanalyzer/rca", target1.getEndpoint());
        target1.setEndpoint("localhost:9650");
        assertEquals("localhost:9650", target1.getEndpoint());
    }

    @Test
    public void parameterTest() {
        assertNull(target2.getParameter());
        assertEquals("ClusterTemperatureRca", target1.getParameter());
        target2.setParameter("HighHeapUsageClusterRca");
        assertEquals("HighHeapUsageClusterRca", target2.getParameter());
    }
}