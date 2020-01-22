package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;

public class NamedAggregateValue extends AggregateValue {
    private String name;

    public NamedAggregateValue(Number value, Statistics type, String name) {
        super(value, type);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void format(Formatter formatter, MeasurementSet measurementSet, Statistics stats) {
        formatter.formatNamedAggregatedValue(measurementSet, getAggregationType(), getName(), getValue());
    }

    public void update(Number value) {
        this.value = value;
    }
}
