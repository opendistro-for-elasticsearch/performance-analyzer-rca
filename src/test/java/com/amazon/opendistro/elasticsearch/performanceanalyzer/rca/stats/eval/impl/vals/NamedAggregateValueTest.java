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

public class NamedAggregateValueTest {
    private static final String NAME = "NAME";
    private static final Integer VALUE = 1;

    private NamedAggregateValue uut;
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
        uut = new NamedAggregateValue(VALUE, aggregationType, NAME);
    }

    @Test
    public void testFormat() {
        uut.format(formatter, measurementSet, statistics);
        // TODO is this correct behavior????? The Statistics parameter in the UUT is unused!
        Mockito.verify(formatter, Mockito.times(1))
                .formatNamedAggregatedValue(measurementSet, aggregationType, NAME, uut.getValue());
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(uut, uut);
        Assert.assertNotEquals(uut, VALUE);
        Assert.assertNotEquals(uut, null);
        NamedAggregateValue that = new NamedAggregateValue(Math.E, aggregationType, NAME);
        Assert.assertEquals(uut.getAggregationType(), that.getAggregationType());
        Assert.assertNotEquals(uut, that);
        Assert.assertNotEquals(that, uut);
        Assert.assertEquals(new NamedAggregateValue(VALUE, aggregationType, NAME), uut);
    }

    @Test
    public void testHashcode() {
        AggregateValue v = new AggregateValue(VALUE, aggregationType);
        Assert.assertEquals(Objects.hash(v.hashCode(), NAME), uut.hashCode());
    }

    @Test
    public void testToString() {
        String expected = "NamedAggregateValue{"
                + "name='"
                + uut.getName()
                + '\''
                + ", aggr='"
                + uut.getAggregationType()
                + '\''
                + ", value="
                + uut.getValue()
                + '}';
        Assert.assertEquals(
                expected, uut.toString());
    }
}