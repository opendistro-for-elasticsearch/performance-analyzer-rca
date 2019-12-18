/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.MetricsDBProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.helpers.OSMetricHelper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class MetricsDBProviderTestHelper extends MetricsDBProvider {

  private MetricsDB db;
  private final String DB_FILENAME = "metricsdb_4_rca.sqlite";

  public MetricsDBProviderTestHelper() throws Exception {
    this(true);
  }

  public MetricsDBProviderTestHelper(boolean fillData) throws Exception {
    System.setProperty("java.io.tmpdir", "/tmp");

    // Cleanup the file if exists.
    try {
      Files.delete(Paths.get(DB_FILENAME));
    } catch (NoSuchFileException ignored) {
    }

    try {
      Files.delete(Paths.get(DB_FILENAME + "-journal"));
    } catch (NoSuchFileException ignored) {
    }

    // TODO: clean up the DB after the tests are done.
    db =
        new MetricsDB(System.currentTimeMillis()) {
          @Override
          public String getDBFilePath() {
            // final String dir = System.getProperty("user.dir");
            Path configPath = Paths.get(DB_FILENAME);
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

    createAllOsMetricsTables();
    if (fillData) {
      fillWithData();
    }
  }

  @Override
  public MetricsDB getMetricsDB() throws Exception {
    return db;
  }

  private void createAllOsMetricsTables() {
    Arrays.stream(AllMetrics.OSMetrics.values())
        .forEach((metric) -> OSMetricHelper.create(db, metric.name()));
  }

  private void fillWithData() {
    int fakeTimeVal = 0;
    for (AllMetrics.OSMetrics metric : AllMetrics.OSMetrics.values()) {
      OSMetricHelper.insert(db, metric.name(), ++fakeTimeVal);
    }
  }

  private void addNewData(String metricName, double value) {
    OSMetricHelper.insert(db, metricName, value);
  }

  public void addNewData(String metricName, List<String> dims, double value) {
    OSMetricHelper.insert(db, metricName, value, dims);
  }

  public void addNewData(String metricName, List<Double> values) {
    values.forEach((a) -> addNewData(metricName, a));
  }
}
