package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;

public class MetricsDBProvider implements Queryable {
  private static final Logger LOG = LogManager.getLogger(MetricsDBProvider.class);

  @Override
  public MetricsDB getMetricsDB() throws Exception {
    ReaderMetricsProcessor processor = ReaderMetricsProcessor.getInstance();
    if (processor == null) {
      LOG.error("RCA: ReaderMetricsProcessor not initialized");
      throw new Exception("ReaderMetricsProcessor not initialized");
    }
    Map.Entry<Long, MetricsDB> dbEntry = processor.getMetricsDB();
    if (dbEntry == null) {
      LOG.error("RCA: MetricsDB not initialized");
      throw new Exception("Metrics DB not initialized");
    }
    return dbEntry.getValue();
  }

  @Override
  public List<List<String>> queryMetrics(MetricsDB db, String metricName) {
    // LOG.info("RCA: About to get the query metric: {}", metricName);
    Result<Record> queryResult = db.queryMetric(metricName);
    // LOG.info("RCA: result {}", queryResult);

    List<List<String>> retResults = new ArrayList<>();
    List<String> columnNames =
        Arrays.stream(queryResult.fields()).map(Field::getName).collect(Collectors.toList());
    retResults.add(columnNames);

    for (Record r : queryResult) {
      List<String> row = new ArrayList<>();
      for (String col : columnNames) {
        row.add(String.valueOf(r.getValue(col)));
      }
      // LOG.info("RCA: row: \n{}", r);
      retResults.add(row);
    }
    return retResults;
  }

  @Override
  public long getDBTimestamp(MetricsDB db) {
    return 0;
    // return db.getWindowStartTime();
  }
}
