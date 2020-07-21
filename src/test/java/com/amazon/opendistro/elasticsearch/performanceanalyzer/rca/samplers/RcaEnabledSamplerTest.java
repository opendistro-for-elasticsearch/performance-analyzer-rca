package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
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
    private AppContext appContext;

    @Mock
    private SampleAggregator sampleAggregator;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        appContext = new AppContext();
        uut = new RcaEnabledSampler(appContext);
    }

    @Test
    public void testIsRcaEnabled() {
        RcaController rcaController = new RcaController();
        PerformanceAnalyzerApp.setRcaController(rcaController);

        assertFalse(uut.isRcaEnabled());
        ClusterDetailsEventProcessor.NodeDetails details =
                ClusterDetailsEventProcessorTestHelper.newNodeDetails("", "", false);

        ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
        clusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(details));
        appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

        assertFalse(uut.isRcaEnabled());
        details = ClusterDetailsEventProcessorTestHelper.newNodeDetails("", "", true);
        clusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(details));
        assertEquals(rcaController.isRcaEnabled(), uut.isRcaEnabled());
    }

    @Test
    public void testSample() {
        RcaController rcaController = new RcaController();
        PerformanceAnalyzerApp.setRcaController(rcaController);

        uut.sample(sampleAggregator);
        verify(sampleAggregator, times(1))
                .updateStat(RcaRuntimeMetrics.RCA_ENABLED, "",
                    PerformanceAnalyzerApp.getRcaController().isRcaEnabled() ? 1 : 0);
    }
}
