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

public class AggregateValueTest {
    private static final Integer VALUE = 1;

    private AggregateValue uut;
    private Statistics aggregationType;
    private Statistics statistics;

    @Mock
    private Formatter formatter;

    @Mock
    private MeasurementSet measurementSet;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        aggregationType = Statistics.MAX;
        statistics = Statistics.MEAN;
        uut = new AggregateValue(VALUE, aggregationType);
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
        AggregateValue that = new AggregateValue(VALUE, Statistics.COUNT);
        AggregateValue that2 = new AggregateValue(Math.E, aggregationType);
        Assert.assertEquals(uut.getAggregationType(), that2.getAggregationType());
        Assert.assertNotEquals(uut, that);
        Assert.assertNotEquals(that, uut);
        Assert.assertNotEquals(uut, that2);
        Assert.assertNotEquals(that2, uut);
        Assert.assertEquals(that2, new AggregateValue(Math.E, aggregationType));
    }

    @Test
    public void testHashcode() {
        Value v = new Value(VALUE);
        Assert.assertEquals(Objects.hash(v.hashCode(), aggregationType), uut.hashCode());
    }

    @Test
    public void testToString() {
        Assert.assertEquals(
                "AggregateValue{" + "aggregationType=" + aggregationType + ", value=" + VALUE + '}', uut.toString());
    }
}