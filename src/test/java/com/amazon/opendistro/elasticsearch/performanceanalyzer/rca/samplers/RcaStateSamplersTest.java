package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class RcaStateSamplersTest {
    private RcaStateSamplers uut;

    @Before
    public void setup() {
        uut = new RcaStateSamplers();
    }

    @Test
    public void testGetRcaEnabledSampler() {
        assertSame(uut.getClass(), RcaStateSamplers.class); // done for constructor coverage
        assertTrue(RcaStateSamplers.getRcaEnabledSampler() instanceof RcaEnabledSampler);
    }
}
