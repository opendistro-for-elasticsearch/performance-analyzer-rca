/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rest;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor.BATCH_METRICS_ENABLED_CONF_FILE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.Dimensions;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.model.MetricsModel;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class QueryBatchRequestHandlerTest {
  private QueryBatchRequestHandler handler;
  private String rootLocation;
  private String queryPrefix;
  private long timestamp;

  @BeforeClass
  public static void setUpClass() throws IOException {
    Files.createDirectories(Paths.get(Util.DATA_DIR));
  }

  @Before
  public void setUp() throws IOException {
    handler = new QueryBatchRequestHandler(null, new MetricsRestUtil());
    rootLocation = "build/resources/test/reader/";
    setBatchMetricsEnabled(false);
    queryPrefix = "http://localhost:9600/_opendistro/_performanceanalyzer/batch?";
    timestamp = 1566413970000L;
  }

  @Test
  public void testHandle_invalidRequestMethod() throws Exception {
    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getResponseBody()).thenReturn(System.out);
    Mockito.when(exchange.getRequestMethod()).thenReturn("POST");

    handler.handle(exchange);
    Mockito.verify(exchange).sendResponseHeaders(ArgumentMatchers.eq(HttpURLConnection.HTTP_NOT_FOUND),
            ArgumentMatchers.anyLong());
  }

  @Test
  public void testHandle_nullMP() throws IOException {
    ReaderMetricsProcessor.setCurrentInstance(null);

    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getResponseBody()).thenReturn(System.out);
    Mockito.when(exchange.getRequestMethod()).thenReturn("GET");

    handler.handle(exchange);
    Mockito.verify(exchange).sendResponseHeaders(ArgumentMatchers.eq(HttpURLConnection.HTTP_UNAVAILABLE),
            ArgumentMatchers.anyLong());
  }

  @Test
  public void testHandle_batchDisabled() throws Exception {
    ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
    ReaderMetricsProcessor.setCurrentInstance(mp);
    setBatchMetricsEnabled(false);

    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getResponseBody()).thenReturn(System.out);
    Mockito.when(exchange.getRequestMethod()).thenReturn("GET");

    handler.handle(exchange);
    Mockito.verify(exchange).sendResponseHeaders(ArgumentMatchers.eq(HttpURLConnection.HTTP_UNAVAILABLE),
            ArgumentMatchers.anyLong());
  }

  @Test
  public void testHandle_batchEmpty() throws Exception {
    ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
    ReaderMetricsProcessor.setCurrentInstance(mp);
    setBatchMetricsEnabled(true);
    mp.trimOldSnapshots();
    mp.trimMetricsDBFiles();

    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getResponseBody()).thenReturn(System.out);
    Mockito.when(exchange.getRequestMethod()).thenReturn("GET");

    handler.handle(exchange);
    Mockito.verify(exchange).sendResponseHeaders(ArgumentMatchers.eq(HttpURLConnection.HTTP_UNAVAILABLE),
            ArgumentMatchers.anyLong());
  }

  @Test
  public void testHandle_emptyQuery() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix);
  }

  @Test
  public void testHandle_invalidParameters() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "foo");
    checkBadQuery(queryPrefix + "foo&bar");
    checkBadQuery(queryPrefix + "foo=");
    checkBadQuery(queryPrefix + "foo=&bar=");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&foo=");
  }

  @Test
  public void testHandle_missingMetric() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "metrics=&starttime=1566413975000&endtime=1566413980000");
    checkBadQuery(queryPrefix + "metrics&starttime=1566413975000&endtime=1566413980000");
    checkBadQuery(queryPrefix + "starttime=1566413975000&endtime=1566413980000");
  }

  @Test
  public void testHandle_invalidMetric() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "metrics=foo&starttime=1566413975000&endtime=1566413980000");
  }

  @Test
  public void testHandle_missingStarttime() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=&endtime=1566413980000");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime&endtime=1566413980000");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&endtime=1566413980000");
  }

  @Test
  public void testHandle_invalidStarttime() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=foo&endtime=1566413980000");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=-1&endtime=1566413980000");
  }

  @Test
  public void testHandle_missingEndtime() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000&endtime=");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000&endtime");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000");
  }

  @Test
  public void testHandle_invalidEndtime() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000&endtime=foo");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000&endtime=-1");
  }

  @Test
  public void testHandle_invalidSamplingPeriod() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000&endtime=1566413980000&samplingperiod=0");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000&endtime=1566413980000&samplingperiod=4");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000&endtime=1566413980000&samplingperiod=6");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413975000&endtime=1566413980000&samplingperiod="
            + PluginSettings.instance().getBatchMetricsRetentionPeriodMinutes() * 60);
  }

  @Test
  public void testHandle_invalidTimes() throws Exception {
    prepareMP();
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413980000&endtime=1566413980000");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=1566413980000&endtime=1566413982000");
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime=" + (System.currentTimeMillis() - 10000)
            + "&endtime=" + (System.currentTimeMillis() + 10000));
    checkBadQuery(queryPrefix + "metrics=CPU_Utilization&starttime="
            + (System.currentTimeMillis() - PluginSettings.instance().getBatchMetricsRetentionPeriodMinutes() * 60 * 1000 - 10000)
            + "&endtime=" + System.currentTimeMillis());
  }

  @Test
  public void testAppendMetrics() throws Exception {
    deleteAll();
    for (int i = 0; i < 8; i++) {
      boolean metricAEnabled = (i & 1) > 0;
      boolean metricBEnabled = (i & 2) > 0;
      boolean metricCEnabled = (i & 4) > 0;
      StringBuilder expectedResponseBuilder = new StringBuilder();
      prepareMetricDB(timestamp, metricAEnabled, metricBEnabled, metricCEnabled, expectedResponseBuilder);
      String expectedResponse = expectedResponseBuilder.toString();
      for (int j = 0; j < 8; j++) {
        boolean queryMetricA = (j & 1) > 0;
        boolean queryMetricB = (j & 2) > 0;
        boolean queryMetricC = (j & 4) > 0;
        if (metricAEnabled && !queryMetricA || metricBEnabled && !queryMetricB || metricCEnabled && !queryMetricC) {
          continue;
        }
        List<String> metrics = new ArrayList<>();
        if (queryMetricA) {
          metrics.add("CPU_Utilization");
        }
        if (queryMetricB) {
          metrics.add("Paging_RSS");
        }
        if (queryMetricC) {
          metrics.add("Sched_Runtime");
        }
        StringBuilder builder = new StringBuilder();
        Assert.assertEquals(0, handler.appendMetricsShim(timestamp, metrics, builder, Integer.bitCount(i & j)));
        Assert.assertEquals(expectedResponse, builder.toString());
      }
      MetricsDB db = MetricsDB.fetchExisting(timestamp);
      db.remove();
      db.deleteOnDiskFile();
    }
  }

  @Test
  public void testAppendMetrics_exceedsMaxDatapoints() throws Exception {
    deleteAll();
    StringBuilder expectedResponseBuilder = new StringBuilder();
    prepareMetricDB(timestamp, true, true, true, expectedResponseBuilder);
    List<String> metrics = Arrays.asList("CPU_Utilization", "Paging_RSS", "Sched_Runtime");
    for (int i = 0; i < 3; i++) {
      try {
        handler.appendMetricsShim(timestamp, metrics, new StringBuilder(), i);
        Assert.fail();
      } catch (InvalidParameterException e) {
      }
    }
    StringBuilder actualResponseBuilder = new StringBuilder();
    handler.appendMetricsShim(timestamp, metrics, actualResponseBuilder, 3);
    Assert.assertEquals(expectedResponseBuilder.toString(), actualResponseBuilder.toString());
  }

  @Test
  public void testQueryFromBatchMetrics_emptyBatchMetrics() throws Exception {
    NavigableSet<Long> batchMetrics = new TreeSet<Long>();
    String response = handler.queryFromBatchMetricsShim(batchMetrics, Arrays.asList("CPU_Utilization"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL, 5000, 1);
    Assert.assertEquals(response, "{}");
  }

  @Test
  public void testQueryFromBatchMetrics_noMetricsInTimerange() throws Exception {
    NavigableSet<Long> batchMetrics = new TreeSet<Long>();
    batchMetrics.add(timestamp - MetricsConfiguration.SAMPLING_INTERVAL);
    String response = handler.queryFromBatchMetricsShim(batchMetrics, Arrays.asList("CPU_Utilization"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL, 5000, 1);
    Assert.assertEquals(response, "{}");
    batchMetrics.clear();
    batchMetrics.add(timestamp + MetricsConfiguration.SAMPLING_INTERVAL);
    response = handler.queryFromBatchMetricsShim(batchMetrics, Arrays.asList("CPU_Utilization"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL, 5000, 1);
    Assert.assertEquals(response, "{}");
    batchMetrics.clear();
    batchMetrics.add(timestamp + 2 * MetricsConfiguration.SAMPLING_INTERVAL);
    response = handler.queryFromBatchMetricsShim(batchMetrics, Arrays.asList("CPU_Utilization"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL, 5000, 1);
    Assert.assertEquals(response, "{}");
  }

  @Test
  public void testQueryFromBatchMetrics_basic() throws Exception {
    deleteAll();
    NavigableSet<Long> batchMetrics = new TreeSet<Long>();
    List<Long> timestamps = Arrays.asList(timestamp, timestamp + MetricsConfiguration.SAMPLING_INTERVAL,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 2,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3);
    batchMetrics.addAll(timestamps);
    StringBuilder expectedResponseBuilder = new StringBuilder();
    expectedResponseBuilder.append("{");
    prepareMetricDB(timestamps.get(0), expectedResponseBuilder);
    expectedResponseBuilder.append(",");
    prepareMetricDB(timestamps.get(1), expectedResponseBuilder);
    expectedResponseBuilder.append(",");
    prepareMetricDB(timestamps.get(2), expectedResponseBuilder);
    expectedResponseBuilder.append("}");
    String response = handler.queryFromBatchMetricsShim(batchMetrics,
            Arrays.asList("CPU_Utilization", "Paging_RSS", "Sched_Runtime"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3, 5000, 9);
    Assert.assertEquals(expectedResponseBuilder.toString(), response);
  }

  @Test
  public void testQueryFromBatchMetrics_basicWithSamplingPeriod10() throws Exception {
    deleteAll();
    NavigableSet<Long> batchMetrics = new TreeSet<Long>();
    List<Long> timestamps = Arrays.asList(timestamp, timestamp + MetricsConfiguration.SAMPLING_INTERVAL,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 2,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3);
    batchMetrics.addAll(timestamps);
    StringBuilder expectedResponseBuilder = new StringBuilder();
    expectedResponseBuilder.append("{");
    prepareMetricDB(timestamps.get(0), expectedResponseBuilder);
    expectedResponseBuilder.append(",");
    prepareMetricDB(timestamps.get(1), new StringBuilder());
    prepareMetricDB(timestamps.get(2), expectedResponseBuilder);
    expectedResponseBuilder.append("}");
    String response = handler.queryFromBatchMetricsShim(batchMetrics,
            Arrays.asList("CPU_Utilization", "Paging_RSS", "Sched_Runtime"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3, 10000, 6);
    Assert.assertEquals(expectedResponseBuilder.toString(), response);
  }

  @Test
  public void testQueryFromBatchMetrics_missingTimestamps() throws Exception {
    deleteAll();
    NavigableSet<Long> batchMetrics = new TreeSet<Long>();
    List<Long> timestamps = Arrays.asList(timestamp, timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3);
    batchMetrics.addAll(timestamps);
    StringBuilder expectedResponseBuilder = new StringBuilder();
    expectedResponseBuilder.append("{");
    prepareMetricDB(timestamps.get(0), expectedResponseBuilder);
    prepareMetricDB(timestamps.get(1), new StringBuilder());
    expectedResponseBuilder.append("}");
    String response = handler.queryFromBatchMetricsShim(batchMetrics,
            Arrays.asList("CPU_Utilization", "Paging_RSS", "Sched_Runtime"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3, 5000, 3);
    Assert.assertEquals(expectedResponseBuilder.toString(), response);
  }

  @Test
  public void testQueryFromBatchMetrics_missingTimestamps2() throws Exception {
    deleteAll();
    NavigableSet<Long> batchMetrics = new TreeSet<Long>();
    List<Long> timestamps = Arrays.asList(timestamp + MetricsConfiguration.SAMPLING_INTERVAL,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 2,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3);
    batchMetrics.addAll(timestamps);
    StringBuilder expectedResponseBuilder = new StringBuilder();
    expectedResponseBuilder.append("{");
    prepareMetricDB(timestamps.get(0), expectedResponseBuilder);
    expectedResponseBuilder.append(",");
    prepareMetricDB(timestamps.get(1), expectedResponseBuilder);
    prepareMetricDB(timestamps.get(2), new StringBuilder());
    expectedResponseBuilder.append("}");
    String response = handler.queryFromBatchMetricsShim(batchMetrics,
            Arrays.asList("CPU_Utilization", "Paging_RSS", "Sched_Runtime"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3, 10000, 6);
    Assert.assertEquals(expectedResponseBuilder.toString(), response);
  }

  @Test
  public void testQueryFromBatchMetrics_missingTimestamps3() throws Exception {
    deleteAll();
    NavigableSet<Long> batchMetrics = new TreeSet<Long>();
    List<Long> timestamps = Arrays.asList(timestamp + MetricsConfiguration.SAMPLING_INTERVAL,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3);
    batchMetrics.addAll(timestamps);
    StringBuilder expectedResponseBuilder = new StringBuilder();
    expectedResponseBuilder.append("{");
    prepareMetricDB(timestamps.get(0), expectedResponseBuilder);
    prepareMetricDB(timestamps.get(1), new StringBuilder());
    expectedResponseBuilder.append("}");
    String response = handler.queryFromBatchMetricsShim(batchMetrics,
            Arrays.asList("CPU_Utilization", "Paging_RSS", "Sched_Runtime"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3, 10000, 3);
    Assert.assertEquals(expectedResponseBuilder.toString(), response);
  }

  @Test
  public void testQueryFromBatchMetrics_missingTimestamps4() throws Exception {
    deleteAll();
    NavigableSet<Long> batchMetrics = new TreeSet<Long>();
    List<Long> timestamps = Arrays.asList(timestamp, timestamp + MetricsConfiguration.SAMPLING_INTERVAL,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 3,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 4);
    batchMetrics.addAll(timestamps);
    StringBuilder expectedResponseBuilder = new StringBuilder();
    expectedResponseBuilder.append("{");
    prepareMetricDB(timestamps.get(0), expectedResponseBuilder);
    prepareMetricDB(timestamps.get(1), new StringBuilder());
    expectedResponseBuilder.append(",");
    prepareMetricDB(timestamps.get(2), expectedResponseBuilder);
    expectedResponseBuilder.append(",");
    prepareMetricDB(timestamps.get(3), expectedResponseBuilder);
    expectedResponseBuilder.append("}");
    String response = handler.queryFromBatchMetricsShim(batchMetrics,
            Arrays.asList("CPU_Utilization", "Paging_RSS", "Sched_Runtime"), timestamp,
            timestamp + MetricsConfiguration.SAMPLING_INTERVAL * 5, 10000, 9);
    Assert.assertEquals(expectedResponseBuilder.toString(), response);
  }

  private void prepareMP() throws Exception {
    ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
    ReaderMetricsProcessor.setCurrentInstance(mp);
    setBatchMetricsEnabled(true);
    mp.trimOldSnapshots();
    mp.trimMetricsDBFiles();
    mp.processMetrics(rootLocation, timestamp);
    mp.trimOldSnapshots();
    mp.trimMetricsDBFiles();
  }

  private void setBatchMetricsEnabled(boolean state) throws IOException {
    Files.write(Paths.get(Util.DATA_DIR, BATCH_METRICS_ENABLED_CONF_FILE),
            Boolean.toString(state).getBytes());
  }

  private HttpExchange sendQuery(String query) throws Exception {
    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getResponseBody()).thenReturn(System.out);
    Mockito.when(exchange.getRequestMethod()).thenReturn("GET");
    Headers responseHeaders = Mockito.mock(Headers.class);
    Mockito.when(exchange.getResponseHeaders()).thenReturn(responseHeaders);
    Mockito.when(exchange.getRequestURI()).thenReturn(new URI(query));

    handler.handle(exchange);
    return exchange;
  }

  private void checkBadQuery(String query) throws Exception {
    HttpExchange exchange = sendQuery(query);
    Mockito.verify(exchange).sendResponseHeaders(ArgumentMatchers.eq(HttpURLConnection.HTTP_BAD_REQUEST),
            ArgumentMatchers.anyLong());
  }

  private Dimensions createDimensionData(String suffix) {
    Dimensions dimensionData = new Dimensions();
    for (String dimension : MetricsModel.ALL_METRICS.get("CPU_Utilization").dimensionNames) {
      dimensionData.put(dimension, dimension + suffix);
    }
    return dimensionData;
  }

  private void prepareMetricDB(long timestamp, boolean metricAEnabled, boolean metricBEnabled,
                                      boolean metricCEnabled, StringBuilder builder) throws Exception {
    MetricsDB db = new MetricsDB(timestamp);
    builder.append("\"");
    builder.append(timestamp);
    builder.append("\":{");
    List<String> dimensions = new ArrayList(MetricsModel.ALL_METRICS.get("CPU_Utilization").dimensionNames);
    if (metricAEnabled) {
      Metric metric = new Metric<Double>("CPU_Utilization", 10D);
      db.createMetric(metric, dimensions);
      db.putMetric(metric, createDimensionData("A"), timestamp);
      builder.append("\"CPU_Utilization\":");
      builder.append(db.queryMetric("CPU_Utilization", MetricsModel.ALL_METRICS.get("CPU_Utilization").dimensionNames,
              1).formatJSON());
    }
    if (metricBEnabled) {
      Metric metric = new Metric<Double>("Paging_RSS", 10D);
      db.createMetric(metric, dimensions);
      db.putMetric(metric, createDimensionData("B"), timestamp);
      if (metricAEnabled) {
        builder.append(",");
      }
      builder.append("\"Paging_RSS\":");
      builder.append(db.queryMetric("Paging_RSS", MetricsModel.ALL_METRICS.get("Paging_RSS").dimensionNames,
              1).formatJSON());
    }
    if (metricCEnabled) {
      Metric metric = new Metric<Double>("Sched_Runtime", 10D);
      db.createMetric(metric, dimensions);
      db.putMetric(metric, createDimensionData("C"), timestamp);
      if (metricAEnabled || metricBEnabled) {
        builder.append(",");
      }
      builder.append("\"Sched_Runtime\":");
      builder.append(db.queryMetric("Sched_Runtime", MetricsModel.ALL_METRICS.get("Sched_Runtime").dimensionNames,
              1).formatJSON());
    }
    builder.append("}");
    db.commit();
    db.remove();
  }

  private void prepareMetricDB(long timestamp, StringBuilder builder) throws Exception {
    prepareMetricDB(timestamp, true, true, true, builder);
  }

  public void deleteAll() {
    final File folder = new File("/tmp");
    final File[] files =
            folder.listFiles(
                    new FilenameFilter() {
                      @Override
                      public boolean accept(final File dir, final String name) {
                        return name.matches("metricsdb_.*");
                      }
                    });
    for (final File file : files) {
      if (!file.delete()) {
        System.err.println("Can't remove " + file.getAbsolutePath());
      }
    }
  }
}