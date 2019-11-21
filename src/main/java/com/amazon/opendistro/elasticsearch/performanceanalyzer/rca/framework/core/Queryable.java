package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import java.util.List;

public interface Queryable {

  MetricsDB getMetricsDB() throws Exception;

  List<List<String>> queryMetrics(MetricsDB db, String metricName);

  List<List<String>> queryMetrics(
      MetricsDB db, String metricName, String dimension, String aggregation);

  long getDBTimestamp(MetricsDB db);
}
