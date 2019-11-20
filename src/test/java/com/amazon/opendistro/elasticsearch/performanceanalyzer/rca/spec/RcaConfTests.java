package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;

@Category(GradleTaskForRca.class)
public class RcaConfTests {
    @Test
    public void testRcaConfRead() {
        RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());

        assertEquals("s3://sifi-store/rcas/", rcaConf.getRcaStoreLoc());
        assertEquals("s3://sifi-store/thresholds/", rcaConf.getThresholdStoreLoc());
        assertEquals(60, rcaConf.getNewRcaCheckPeriocicityMins());
        assertEquals(30, rcaConf.getNewThresholdCheckPeriodicityMins());
        assertEquals(Arrays.asList("ip1", "ip2", "ip3"), rcaConf.getPeerIpList());
        Map<String, String> tagMap = newHashMap();
        tagMap.put("locus", "data-node");
        tagMap.put("disk", "ssd");
        tagMap.put("region", "use1");
        tagMap.put("instance-type", "i3.8xl");
        tagMap.put("domain", "rca-test-cluster");
        for (Map.Entry<String, String> tag: rcaConf.getTagMap().entrySet()) {
            String expectedValue = tagMap.get(tag.getKey());
            assertEquals(expectedValue, tag.getValue());
        }
    }
}
