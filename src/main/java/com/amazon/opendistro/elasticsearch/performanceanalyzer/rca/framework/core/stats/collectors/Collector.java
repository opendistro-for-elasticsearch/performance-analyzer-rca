package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.Formatter;

public interface Collector {
    /**
     * This gets the current set of Measurements collected and re-initiates the objects for the next
     * iteration.
     *
     * @param formatter An class that knows how to format a map of enum and lists.
     */
    public void fillValuesAndReset(Formatter formatter);
}
