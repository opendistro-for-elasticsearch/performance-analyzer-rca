package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RcaStateSamplersTest {
    private RcaStateSamplers uut;

    @Test
    public void testGetRcaEnabledSampler() {  // done for constructor coverage
        uut = new RcaStateSamplers();
        assertSame(uut.getClass(), RcaStateSamplers.class);
        assertTrue(RcaStateSamplers.getRcaEnabledSampler(new AppContext()) instanceof RcaEnabledSampler);
    }
}
