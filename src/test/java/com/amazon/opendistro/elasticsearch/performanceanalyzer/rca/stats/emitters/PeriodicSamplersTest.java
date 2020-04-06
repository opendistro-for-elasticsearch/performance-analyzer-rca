package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PeriodicSamplersTest {
    private PeriodicSamplers uut;

    @Mock
    private SampleAggregator sampleAggregator;

    @Mock
    private ISampler exceptionSampler;

    @Mock
    private ISampler workingSampler;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        doThrow(new RuntimeException("generic exception")).when(exceptionSampler).sample(any(SampleAggregator.class));
        doNothing().when(workingSampler).sample(any(SampleAggregator.class));
        List<ISampler> samplers = Lists.newArrayList(workingSampler, exceptionSampler);
        uut = new PeriodicSamplers(sampleAggregator, samplers, 5, TimeUnit.SECONDS);
        doNothing().when(sampleAggregator).updateStat(any(MeasurementSet.class), anyString(), any(Number.class));
    }

    @Test
    public void testRun() {
        uut.run();
        // Each sampler should be called twice, once on construction, and once during the run() invocation
        verify(exceptionSampler, times(2)).sample(any(SampleAggregator.class));
        verify(workingSampler, times(2)).sample(any(SampleAggregator.class));
    }

    @Test
    public void testHeartbeat() throws InterruptedException {
        Thread t = uut.startHeartbeat();
        Thread.sleep(2000L);
        assertTrue(t.isAlive());
        uut.getSamplerHandle().cancel(true);
        Thread.sleep(2000L);
        assertEquals(Thread.State.TERMINATED, Thread.State.TERMINATED);
    }
}
