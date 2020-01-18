package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.Value;
import java.util.Collections;
import java.util.List;

public class Sample implements StatisticImpl<Value> {
    private Number value;
    private boolean empty;

    public Sample() {
        empty = true;
    }

    @Override
    public Statistics type() {
        return Statistics.SAMPLE;
    }

    @Override
    public void calculate(String key, Number value) {
        this.value = value;
        empty = false;
    }

    @Override
    public List<Value> get() {
        return Collections.singletonList(new Value(value));
    }

    @Override
    public boolean empty() {
        return empty;
    }
}
