package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Category(GradleTaskForRca.class)
@RunWith(PowerMockRunner.class)
@PrepareForTest({ClusterDetailsEventProcessor.class, RcaController.class})
@PowerMockIgnore({"javax.management.*", "javax.script.*", "com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "org.w3c.dom.*"})
public class RcaEnabledSamplerTest {
    private RcaEnabledSampler uut;

    @Mock
    private SampleAggregator sampleAggregator;

    @Before
    public void setup() {
        mockStatic(ClusterDetailsEventProcessor.class, RcaController.class);
        uut = new RcaEnabledSampler();
    }

    @Test
    public void testIsRcaEnabled() {
        when(ClusterDetailsEventProcessor.getCurrentNodeDetails()).thenReturn(null);
        assertEquals(0, uut.isRcaEnabled());
        ClusterDetailsEventProcessor.NodeDetails details =
                new ClusterDetailsEventProcessor.NodeDetails("", "", "", false);
        when(ClusterDetailsEventProcessor.getCurrentNodeDetails()).thenReturn(details);
        assertEquals(0, uut.isRcaEnabled());
        details = new ClusterDetailsEventProcessor.NodeDetails("", "", "", true);
        when(RcaController.isRcaEnabled()).thenReturn(false);
        when(ClusterDetailsEventProcessor.getCurrentNodeDetails()).thenReturn(details);
        assertEquals(0, uut.isRcaEnabled());
        when(RcaController.isRcaEnabled()).thenReturn(true);
        assertEquals(1, uut.isRcaEnabled());
    }

    @Test
    public void testSample() {
        ClusterDetailsEventProcessor.NodeDetails details =
                new ClusterDetailsEventProcessor.NodeDetails("", "", "", true);
        when(ClusterDetailsEventProcessor.getCurrentNodeDetails()).thenReturn(details);
        when(RcaController.isRcaEnabled()).thenReturn(true);
        uut.sample(sampleAggregator);
        verify(sampleAggregator, times(1)).updateStat(RcaRuntimeMetrics.RCA_ENABLED, "", 1);
        when(RcaController.isRcaEnabled()).thenReturn(false);
        uut.sample(sampleAggregator);
        verify(sampleAggregator, times(1)).updateStat(RcaRuntimeMetrics.RCA_ENABLED, "", 0);
    }
}
