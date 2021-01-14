package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.overrides;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.Dimensions;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.MetricsDBProvider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class RcaItMetricsDBProvider extends MetricsDBProvider {
  private final String DB_FILE_PATH;
  private final MetricsDB db;

  public RcaItMetricsDBProvider(String metricsDbFilePath) throws Exception {
    DB_FILE_PATH = metricsDbFilePath;
    // Cleanup the file if exists.
    try {
      Files.delete(Paths.get(DB_FILE_PATH));
    } catch (NoSuchFileException ignored) {
    }

    try {
      Files.delete(Paths.get(DB_FILE_PATH + "-journal"));
    } catch (NoSuchFileException ignored) {
    }

    // TODO: clean up the DB after the tests are done.
    db = new MetricsDB(System.currentTimeMillis()) {
      @Override
      public String getDBFilePath() {
        Path configPath = Paths.get(DB_FILE_PATH);
        return configPath.toString();
      }

      @Override
      public void deleteOnDiskFile() {
        try {
          Files.delete(Paths.get(getDBFilePath()));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
  }

  @Override
  public MetricsDB getMetricsDB() {
    return db;
  }

  public void insertRow(String metricName,
                        String[] dimensionNames,
                        String[] dimensionValues,
                        double min, double max, double avg, double sum) {
    Dimensions dimensions = new Dimensions();
    for (int i = 0; i < dimensionNames.length; i++) {
      dimensions.put(dimensionNames[i], dimensionValues[i]);
    }
    Metric metric = new Metric(metricName, sum, avg, min, max);

    db.createMetric(metric, Arrays.asList(dimensionNames));
    db.putMetric(metric, dimensions, 0);
  }

  public void clearTable(String metricName) {
    db.deleteMetric(metricName);
  }
}
