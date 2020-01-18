package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;

public class AggregateValue extends Value {
    private Statistics aggregationType;

    public AggregateValue(Number value, Statistics type) {
        super(value);
        this.aggregationType = type;
    }

    public void format(Formatter formatter, MeasurementSet measurementSet, Statistics stats) {
        formatter.formatAggregatedValue(measurementSet, stats, value);
    }

    public Statistics getAggregationType() {
        return aggregationType;
    }
}
