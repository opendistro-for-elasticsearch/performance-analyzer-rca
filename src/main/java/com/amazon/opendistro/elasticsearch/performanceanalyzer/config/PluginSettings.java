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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ConfigStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.util.Properties;

import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;

public class PluginSettings {
  private static final Logger LOG = LogManager.getLogger(PluginSettings.class);

  private static PluginSettings instance;
  public static final String CONFIG_FILES_PATH = "pa_config/";
  private static final String DEFAULT_CONFIG_FILE_PATH =
      Util.PLUGIN_LOCATION + "pa_config/performance-analyzer.properties";
  private static final String METRICS_LOCATION_KEY = "metrics-location";
  private static final String METRICS_LOCATION_DEFAULT = "/dev/shm/performanceanalyzer/";
  private static final String DELETION_INTERVAL_KEY = "metrics-deletion-interval";
  private static final int DELETION_INTERVAL_DEFAULT = 1;
  private static final int WRITER_QUEUE_SIZE_DEFAULT = 100000;
  private static final int DELETION_INTERVAL_MIN = 1;
  private static final int DELETION_INTERVAL_MAX = 60;
  private static final String HTTPS_ENABLED = "https-enabled";
  private static final String WRITER_QUEUE_SIZE = "writer-queue-size";
  private static final String BATCH_METRICS_RETENTION_PERIOD_MINUTES = "batch-metrics-retention-period-minutes";
  private static final long BATCH_METRICS_RETENTION_PERIOD_MINUTES_DEFAULT = 7;
  private static final long BATCH_METRICS_RETENTION_PERIOD_MINUTES_MIN = 1;
  private static final long BATCH_METRICS_RETENTION_PERIOD_MINUTES_MAX = 60;
  public static final String RPC_PORT_CONF_NAME = "rpc-port";
  public static final int RPC_DEFAULT_PORT = 9650;
  public static final String WEBSERVICE_PORT_CONF_NAME = "webservice-listener-port";
  public static final int WEBSERVICE_DEFAULT_PORT = 9600;

  /** Determines whether the metricsdb files should be cleaned up. */
  public static final String DB_FILE_CLEANUP_CONF_NAME = "cleanup-metrics-db-files";

  private String metricsLocation;
  private int metricsDeletionInterval;
  private int writerQueueSize;

  /** If set to true, the metricsdb files are cleaned up, or else the on-disk files are left out. */
  private boolean shouldCleanupMetricsDBFiles;

  private boolean httpsEnabled;
  private Properties settings;
  private final String configFilePath;

  /** Determines how many minutes worth of metricsdb files will be retained if batch metrics is enabled. */
  private long batchMetricsRetentionPeriodMinutes;

  private int rpcPort;
  private int webServicePort;

  static {
    Util.invokePrivilegedAndLogError(PluginSettings::createInstance);
  }

  public String getMetricsLocation() {
    return metricsLocation;
  }

  public void setMetricsLocation(final String metricsLocation) {
    this.metricsLocation = metricsLocation;
  }

  public int getMetricsDeletionInterval() {
    return metricsDeletionInterval * 60 * 1000;
  }

  public int getWriterQueueSize() {
    return writerQueueSize;
  }

  public long getBatchMetricsRetentionPeriodMinutes() {
    return batchMetricsRetentionPeriodMinutes;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public int getWebServicePort() {
    return webServicePort;
  }

  @VisibleForTesting
  public void setBatchMetricsRetentionPeriodMinutes(long batchMetricsRetentionPeriodMinutes) {
    this.batchMetricsRetentionPeriodMinutes = batchMetricsRetentionPeriodMinutes;
  }

  public String getSettingValue(String settingName) {
    return settings.getProperty(settingName);
  }

  public String getSettingValue(String settingName, String defaultValue) {
    return settings.getProperty(settingName, defaultValue);
  }

  private void loadHttpsEnabled() throws Exception {
    String httpsEnabledString = settings.getProperty(HTTPS_ENABLED, "False");
    if (httpsEnabledString == null) {
      httpsEnabled = false;
    }
    try {
      httpsEnabled = Boolean.parseBoolean(httpsEnabledString);
    } catch (Exception ex) {
      LOG.error("Unable to parse httpsEnabled property with value {}", httpsEnabledString);
      httpsEnabled = false;
    }
  }

  public boolean getHttpsEnabled() {
    return this.httpsEnabled;
  }

  @VisibleForTesting
  public void setHttpsEnabled(boolean httpsEnabled) {
    this.httpsEnabled = httpsEnabled;
  }

  @VisibleForTesting
  public void overrideProperty(String key, String value) {
    settings.setProperty(key, value);
  }

  @VisibleForTesting
  @Nullable
  public String getProperty(String key) {
    return settings.getProperty(key);
  }

  public boolean shouldCleanupMetricsDBFiles() {
    return shouldCleanupMetricsDBFiles;
  }

  @VisibleForTesting
  public void setShouldCleanupMetricsDBFiles(boolean shouldCleanupMetricsDBFiles) {
    this.shouldCleanupMetricsDBFiles = shouldCleanupMetricsDBFiles;
  }

  private PluginSettings(String cfPath) {
    metricsLocation = METRICS_LOCATION_DEFAULT;
    metricsDeletionInterval = DELETION_INTERVAL_DEFAULT;
    writerQueueSize = WRITER_QUEUE_SIZE_DEFAULT;
    batchMetricsRetentionPeriodMinutes = BATCH_METRICS_RETENTION_PERIOD_MINUTES_DEFAULT;
    rpcPort = RPC_DEFAULT_PORT;
    webServicePort = WEBSERVICE_DEFAULT_PORT;
    System.out.println("Here We arex1\n");
    if (cfPath == null || cfPath.isEmpty()) {
      this.configFilePath = DEFAULT_CONFIG_FILE_PATH;
    } else {
      this.configFilePath = cfPath;
    }

    settings = new Properties();
    try {
      settings = getSettingsFromFile(this.configFilePath);
      loadMetricsDeletionIntervalFromConfig();
      loadMetricsLocationFromConfig();
      loadWriterQueueSizeFromConfig();
      loadHttpsEnabled();
      loadMetricsDBFilesCleanupEnabled();
      loadBatchMetricsRetentionPeriodMinutesFromConfig();
      loadPortsFromConfig();
    } catch (ConfigFileException e) {
      LOG.error(
          "Loading config file {} failed with error: {}. Disabling plugin.",
          this.configFilePath,
          e.toString());
      ConfigStatus.INSTANCE.setConfigurationInvalid();
    } catch (ConfigFatalException e) {
      LOG.error("Having issue to load all config items. Disabling plugin.", e);
      ConfigStatus.INSTANCE.setConfigurationInvalid();
    } catch (Exception e) {
      LOG.error("Unexpected exception while initializing config. Disabling plugin.", e);
      ConfigStatus.INSTANCE.setConfigurationInvalid();
    }
    LOG.info(
        "Config: metricsLocation: {}, metricsDeletionInterval: {}, httpsEnabled: {},"
            + " cleanup-metrics-db-files: {}, batch-metrics-retention-period-minutes: {}, rpc-port: {}, " +
                "webservice-port {}",
        metricsLocation,
        metricsDeletionInterval,
        httpsEnabled,
        shouldCleanupMetricsDBFiles,
        batchMetricsRetentionPeriodMinutes,
        rpcPort,
        webServicePort);
  }

  public static PluginSettings instance() {
    return instance;
  }

  private static void createInstance() {
    String cfPath = System.getProperty("configFilePath");
    instance = new PluginSettings(cfPath);
  }

  private static Properties getSettingsFromFile(String filePath) throws ConfigFileException {
    try {
      return SettingsHelper.getSettings(filePath);
    } catch (Exception e) {
      throw new ConfigFileException(e);
    }
  }

  private void loadMetricsLocationFromConfig() throws ConfigFatalException {
    if (!settings.containsKey(METRICS_LOCATION_KEY)) {
      LOG.info("Cannot find metrics-location, using default value. {}", METRICS_LOCATION_DEFAULT);
    }

    metricsLocation = settings.getProperty(METRICS_LOCATION_KEY, METRICS_LOCATION_DEFAULT);
    validateOrCreateDir(metricsLocation);
  }

  private static void validateOrCreateDir(String path) throws ConfigFatalException {
    File dict = new File(path);

    boolean dictCreated = true;
    if (!dict.exists()) {
      dictCreated = dict.mkdir();
      LOG.info("Trying to create directory {}.", path);
    }

    boolean valid = dictCreated && dict.isDirectory() && dict.canWrite();
    if (!valid) {
      LOG.error(
          "Invalid metrics location {}."
              + " Created: {} (Expect True), Directory: {} (Expect True),"
              + " CanWrite: {} (Expect True)",
          path,
          dict.exists(),
          dict.isDirectory(),
          dict.canWrite());
      throw new ConfigFatalException("Having issue to use path: " + path);
    }
  }

  private void loadMetricsDeletionIntervalFromConfig() {
    if (!settings.containsKey(DELETION_INTERVAL_KEY)) {
      return;
    }

    try {
      int interval = Integer.parseInt(settings.getProperty(DELETION_INTERVAL_KEY));
      if (interval < DELETION_INTERVAL_MIN || interval > DELETION_INTERVAL_MAX) {
        LOG.error(
            "metrics-deletion-interval out of range. Value should in ({}-{}). Using default value {}.",
            DELETION_INTERVAL_MIN,
            DELETION_INTERVAL_MAX,
            metricsDeletionInterval);
        return;
      }
      metricsDeletionInterval = interval;
    } catch (NumberFormatException e) {
      LOG.error(
          (Supplier<?>)
              () ->
                  new ParameterizedMessage(
                      "Invalid metrics-deletion-interval. Using default value {}.",
                      metricsDeletionInterval),
          e);
    }
  }

  private void loadWriterQueueSizeFromConfig() {
    if (!settings.containsKey(WRITER_QUEUE_SIZE)) {
      return;
    }

    try {
      int interval = Integer.parseInt(settings.getProperty(WRITER_QUEUE_SIZE));
      writerQueueSize = interval;
    } catch (NumberFormatException e) {
      LOG.error(
          (Supplier<?>)
              () ->
                  new ParameterizedMessage(
                      "Invalid writer-queue-size. Using default value {}.", writerQueueSize),
          e);
    }
  }

  private void loadMetricsDBFilesCleanupEnabled() {
    String cleanupEnabledString = settings.getProperty(DB_FILE_CLEANUP_CONF_NAME, "True");
    try {
      shouldCleanupMetricsDBFiles = Boolean.parseBoolean(cleanupEnabledString);
    } catch (Exception ex) {
      LOG.error(
          "Unable to parse {} property with value {}. Only true/false expected.",
          DB_FILE_CLEANUP_CONF_NAME,
          cleanupEnabledString);

      // In case of exception, we go with the safe default that the files will always be cleaned up.
      shouldCleanupMetricsDBFiles = true;
    }
  }

  private void loadBatchMetricsRetentionPeriodMinutesFromConfig() {
    if (!settings.containsKey(BATCH_METRICS_RETENTION_PERIOD_MINUTES)) {
      return;
    }

    try {
      long parsedRetentionPeriod = Long.parseLong(settings.getProperty(BATCH_METRICS_RETENTION_PERIOD_MINUTES));
      if (parsedRetentionPeriod < BATCH_METRICS_RETENTION_PERIOD_MINUTES_MIN
              || parsedRetentionPeriod > BATCH_METRICS_RETENTION_PERIOD_MINUTES_MAX) {
        LOG.error("{} out of range. Value should be in range [{}, {}]. Using default value {}.",
                BATCH_METRICS_RETENTION_PERIOD_MINUTES,
                BATCH_METRICS_RETENTION_PERIOD_MINUTES_MIN,
                BATCH_METRICS_RETENTION_PERIOD_MINUTES_MAX,
                batchMetricsRetentionPeriodMinutes);
        return;
      }
      batchMetricsRetentionPeriodMinutes = parsedRetentionPeriod;
    } catch (NumberFormatException e) {
      LOG.error("Invalid batch-metrics-retention-period-minutes {}. Using default value {}.",
              settings.getProperty(BATCH_METRICS_RETENTION_PERIOD_MINUTES),
              batchMetricsRetentionPeriodMinutes);
    }
  }

  public void loadPortsFromConfig() {
    try {
      String rpcPortValue = settings.getProperty(RPC_PORT_CONF_NAME);
      String webServicePortValue = settings.getProperty(WEBSERVICE_PORT_CONF_NAME);
      if (rpcPortValue == null) {
        LOG.info(
                "{} not configured; using default value: {}",
                RPC_PORT_CONF_NAME,
                RPC_DEFAULT_PORT);
        this.rpcPort = RPC_DEFAULT_PORT;
      } else {
        this.rpcPort = Integer.parseInt(rpcPortValue);
      }
      if (webServicePortValue == null) {
        LOG.info(
                "{} not configured; using default value: {}",
                WEBSERVICE_PORT_CONF_NAME,
                WEBSERVICE_DEFAULT_PORT);
        this.webServicePort = WEBSERVICE_DEFAULT_PORT;
      } else {
        this.webServicePort = Integer.parseInt(webServicePortValue);
      }
    } catch (Exception ex) {
      LOG.error(
              "Invalid Configuration: {} Using default value: {} AND Error: {}",
              RPC_PORT_CONF_NAME,
              RPC_DEFAULT_PORT,
              ex.toString());
      this.rpcPort = RPC_DEFAULT_PORT;
      this.webServicePort = WEBSERVICE_DEFAULT_PORT;
    }
  }
}
