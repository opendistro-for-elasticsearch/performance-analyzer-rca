package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.List;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;

public interface Queryable {

    MetricsDB getMetricsDB() throws Exception;

    List<List<String>> queryMetrics(MetricsDB db, String metricName);

    List<List<String>> queryMetrics(MetricsDB db, String metricName, String dimension, String aggregation);

    long getDBTimestamp(MetricsDB db);
}
