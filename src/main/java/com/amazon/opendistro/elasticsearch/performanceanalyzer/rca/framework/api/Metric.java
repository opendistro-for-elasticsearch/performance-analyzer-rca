package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.LeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Metric extends LeafNode {
  static final String[] metricList;
  private static final Logger LOG = LogManager.getLogger(Metric.class);

  static {
    AllMetrics.OSMetrics[] osMetrics = AllMetrics.OSMetrics.values();
    metricList = new String[osMetrics.length];
    for (int i = 0; i < osMetrics.length; ++i) {
      metricList[i] = osMetrics[i].name();
    }
  }

  private String name;

  public Metric(String name, long evaluationIntervalSeconds) {
    super(0, evaluationIntervalSeconds);
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  public FlowUnit gather(Queryable queryable) {
    MetricsDB db;
    try {
      db = queryable.getMetricsDB();
    } catch (Exception e) {
      // TODO: Emit log/stats that gathering failed.
      LOG.error("RCA: Caught an exception while getting the DB {}", e.getMessage());
      return FlowUnit.generic();
    }
    List<List<String>> result = queryable.queryMetrics(db, name);
    // LOG.info("RCA: Metrics from MetricsDB {}", result);
    return new FlowUnit(queryable.getDBTimestamp(db), result, Collections.emptyMap());
  }
}
