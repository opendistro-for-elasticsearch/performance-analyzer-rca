package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RcaEnabledSamplerTest {
    private RcaEnabledSampler uut;

    @Mock
    private SampleAggregator sampleAggregator;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        uut = new RcaEnabledSampler();
    }

    @Test
    public void testIsRcaEnabled() {
        assertFalse(uut.isRcaEnabled());
        ClusterDetailsEventProcessor.NodeDetails details =
                ClusterDetailsEventProcessorTestHelper.newNodeDetails("", "", false);
        ClusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(details));
        assertFalse(uut.isRcaEnabled());
        details = ClusterDetailsEventProcessorTestHelper.newNodeDetails("", "", true);
        ClusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(details));
        assertEquals(RcaController.isRcaEnabled(), uut.isRcaEnabled());
    }

    @Test
    public void testSample() {
        ClusterDetailsEventProcessor.NodeDetails details =
                ClusterDetailsEventProcessorTestHelper.newNodeDetails("", "", true);
        uut.sample(sampleAggregator);
        verify(sampleAggregator, times(1))
                .updateStat(RcaRuntimeMetrics.RCA_ENABLED, "", RcaController.isRcaEnabled() ? 1 : 0);
    }
}
