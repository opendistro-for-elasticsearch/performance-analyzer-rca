package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MalformedAnalysisGraphTest {
    private static final String MSG = "MSG";

    private MalformedAnalysisGraph uut;

    @Before
    public void setup() {
        uut = new MalformedAnalysisGraph(MSG);
    }

    @Test
    public void testConstruction() {
        Assert.assertEquals(MSG, uut.getMessage());
    }
}
