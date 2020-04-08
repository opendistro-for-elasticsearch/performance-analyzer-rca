package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ValueTest {
    private static final Integer VALUE = 1;

    private Value uut;
    private Statistics statistics;

    @Mock
    private Formatter formatter;

    @Mock
    private MeasurementSet measurementSet;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        statistics = Statistics.MEAN;
        uut = new Value(VALUE);
    }

    @Test
    public void testFormat() {
        uut.format(formatter, measurementSet, statistics);
        Mockito.verify(formatter, Mockito.times(1))
                .formatAggregatedValue(measurementSet, statistics, uut.getValue());
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(uut, uut);
        Assert.assertNotEquals(uut, VALUE);
        Assert.assertNotEquals(uut, null);
        Value that = new Value(Math.E);
        Assert.assertNotEquals(uut, that);
        Assert.assertNotEquals(that, uut);
        Assert.assertEquals(that, new Value(Math.E));
    }

    @Test
    public void testHashcode() {
        Assert.assertEquals(Objects.hash(VALUE), uut.hashCode());
    }

    @Test
    public void testToString() {
        Assert.assertEquals("Value{" + "value=" + VALUE + '}', uut.toString());
    }
}