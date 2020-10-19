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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.CacheActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.QueueActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.DeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaControllerHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.FieldDataCacheRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HeapSizeIncreasePolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageOldGenRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageYoungGenRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighOldGenOccupancyRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotNodeClusterRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotShardClusterRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotShardRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.OldGenContendedRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.QueueRejectionRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.ShardRequestCacheRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BasicBucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RcaConf {

  protected String configFileLoc;
  protected long lastModifiedTime;
  protected volatile ConfJsonWrapper conf;

  protected static RcaConf instance;

  private final ObjectMapper mapper;
  private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerApp.class);

  private Map<String, BucketCalculator> tunableResourceToUsageBucket;

  public RcaConf(String configPath) {
    this.configFileLoc = configPath;
    this.tunableResourceToUsageBucket = new HashMap<>();
    JsonFactory factory = new JsonFactory();
    factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
    this.mapper = new ObjectMapper(factory);
    mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    try {
      File configFile = new File(this.configFileLoc);
      this.lastModifiedTime = configFile.lastModified();
      this.conf = mapper.readValue(configFile, ConfJsonWrapper.class);
    } catch (IOException e) {
      LOG.error("Couldn't deserialize" + e.getMessage(), e);
    }
  }

  @VisibleForTesting
  // This should only be used for Tests.
  public RcaConf() {
    this.mapper = new ObjectMapper();
    this.tunableResourceToUsageBucket = new HashMap<>();
  }

  /**
   * Converts json config passed as String to a Java Map.
   *
   * <p>This method should only be called from Tests to parse test configs
   */
  @VisibleForTesting
  public void readConfigFromString(String configJson) throws JsonProcessingException {
    this.conf = mapper.readValue(configJson, ConfJsonWrapper.class);
  }

  public static void clear() {
    instance = null;
  }

  public String getRcaStoreLoc() {
    return conf.getRcaStoreLoc();
  }

  public String getThresholdStoreLoc() {
    return conf.getThresholdStoreLoc();
  }

  public long getNewRcaCheckPeriodicityMins() {
    return conf.getNewRcaCheckPeriodicityMins();
  }

  public long getNewThresholdCheckPeriodicityMins() {
    return conf.getNewThresholdCheckPeriodicityMins();
  }

  public List<String> getPeerIpList() {
    return conf.getPeerIpList();
  }

  public Map<String, String> getTagMap() {
    return conf.getTagMap();
  }

  public Map<String, String> getDatastore() {
    return conf.getDatastore();
  }

  public String getConfigFileLoc() {
    return configFileLoc;
  }

  // Returns the last modified time of Rca Conf file
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public String getAnalysisGraphEntryPoint() {
    return conf.getAnalysisGraphEntryPoint();
  }

  public int getNetworkQueueLength() {
    return conf.getNetworkQueueLength();
  }

  public int getPerVertexBufferLength() {
    return conf.getPerVertexBufferLength();
  }

  public HighOldGenOccupancyRcaConfig getHighOldGenOccupancyRcaConfig() {
    return new HighOldGenOccupancyRcaConfig(this);
  }

  public HighHeapUsageOldGenRcaConfig getHighHeapUsageOldGenRcaConfig() {
    return new HighHeapUsageOldGenRcaConfig(this);
  }

  public HighHeapUsageYoungGenRcaConfig getHighHeapUsageYoungGenRcaConfig() {
    return new HighHeapUsageYoungGenRcaConfig(this);
  }

  public QueueRejectionRcaConfig getQueueRejectionRcaConfig() {
    return new QueueRejectionRcaConfig(this);
  }

  public HotNodeClusterRcaConfig getHotNodeClusterRcaConfig() {
    return new HotNodeClusterRcaConfig(this);
  }

  public HotShardRcaConfig getHotShardRcaConfig() {
    return new HotShardRcaConfig(this);
  }

  public HotShardClusterRcaConfig getHotShardClusterRcaConfig() {
    return new HotShardClusterRcaConfig(this);
  }

  public FieldDataCacheRcaConfig getFieldDataCacheRcaConfig() {
    return new FieldDataCacheRcaConfig(this);
  }

  public ShardRequestCacheRcaConfig getShardRequestCacheRcaConfig() {
    return new ShardRequestCacheRcaConfig(this);
  }

  public DeciderConfig getDeciderConfig() {
    return new DeciderConfig(this);
  }

  public List<String> getMutedRcaList() {
    return conf.getMutedRcaList();
  }

  public List<String> getMutedDeciderList() {
    return conf.getMutedDeciderList();
  }

  public List<String> getMutedActionList() {
    return conf.getMutedActionList();
  }

  public Map<String, Object> getRcaConfigSettings() {
    return ImmutableMap.copyOf(conf.getRcaConfigSettings());
  }

  public CacheActionConfig getCacheActionConfig() {
    return new CacheActionConfig(this);
  }

  public QueueActionConfig getQueueActionConfig() {
    return new QueueActionConfig(this);
  }

  public HeapSizeIncreasePolicyConfig getJvmScaleUpPolicyConfig() {
    return new HeapSizeIncreasePolicyConfig(this);
  }

  public OldGenContendedRcaConfig getOldGenContendedRcaConfig() {
    return new OldGenContendedRcaConfig(this);
  }

  public <T> T readRcaConfig(String rcaName, String key, T defaultValue, Class<? extends T> clazz) {
    return readRcaConfig(rcaName, key, defaultValue, (s) -> true, clazz);
  }

  @SuppressWarnings("unchecked")
  public <T> T readRcaConfig(String rcaName, String key, T defaultValue, Predicate<T> validator, Class<? extends T> clazz) {
    T setting = defaultValue;
    try {
      Map<String, Object> rcaObj = null;
      if (conf.getRcaConfigSettings() != null
          && conf.getRcaConfigSettings().containsKey(rcaName)
          && conf.getRcaConfigSettings().get(rcaName) != null) {
        rcaObj = (Map<String, Object>) conf.getRcaConfigSettings().get(rcaName);
      }

      if (rcaObj != null
          && rcaObj.containsKey(key)
          && rcaObj.get(key) != null) {
        setting = clazz.cast(rcaObj.get(key));
        if (!validator.test(setting)) {
          LOG.error("Config value: [{}] provided for key: [{}] is invalid", setting, key);
          return defaultValue;
        }
      }
    } catch (ClassCastException ne) {
      LOG.error("rca.conf contains value in invalid format, trace : {}", ne.getMessage());
    }
    return setting;
  }

  public boolean updateAllRcaConfFiles(final Set<String> mutedRcas, final Set<String> mutedDeciders,
                                       final Set<String> mutedActions) {
    boolean updateStatus = true;
    // update all rca.conf files
    List<String> rcaConfFiles = RcaControllerHelper.getAllConfFilePaths();
    for (String confFilePath : rcaConfFiles) {
      updateStatus = updateRcaConf(confFilePath, mutedRcas, mutedDeciders,
          mutedActions);
      if (!updateStatus) {
        LOG.error("Failed to update the conf file at path: {}", confFilePath);
        StatsCollector.instance().logMetric(RcaConsts.WRITE_UPDATED_RCA_CONF_ERROR);
        break;
      }
    }
    return updateStatus;
  }

  private boolean updateRcaConf(String originalFilePath, final Set<String> mutedRcas,
                                final Set<String> mutedDeciders, final Set<String> mutedActions) {

    String updatedPath = originalFilePath + ".updated";
    try (final FileInputStream originalFileInputStream = new FileInputStream(originalFilePath);
         final Scanner scanner = new Scanner(originalFileInputStream, StandardCharsets.UTF_8.name());
         final FileOutputStream updatedFileOutputStream = new FileOutputStream(updatedPath)) {
      // create the config json Object from rca config file
      String jsonText = scanner.useDelimiter("\\A").next();
      ObjectMapper mapper = new ObjectMapper();
      mapper.enable(JsonParser.Feature.ALLOW_COMMENTS);
      mapper.enable(SerializationFeature.INDENT_OUTPUT);
      JsonNode configObject = mapper.readTree(jsonText);

      ArrayNode mutedRcasArray = mapper.valueToTree(mutedRcas);
      ArrayNode mutedDecidersArray = mapper.valueToTree(mutedDeciders);
      ArrayNode mutedActionsArray = mapper.valueToTree(mutedActions);
      ((ObjectNode) configObject).putArray("muted-rcas").addAll(mutedRcasArray);
      ((ObjectNode) configObject).putArray("muted-deciders").addAll(mutedDecidersArray);
      ((ObjectNode) configObject).putArray("muted-actions").addAll(mutedActionsArray);

      mapper.writeValue(updatedFileOutputStream, configObject);
    } catch (IOException e) {
      LOG.error("Unable to copy rca conf to a temp file", e);
      return false;
    }

    try {
      LOG.info("Writing new file: {}", Paths.get(updatedPath));
      Files
          .move(Paths.get(updatedPath), Paths.get(originalFilePath), StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOG.error("Unable to move and replace the old conf file with updated conf file.", e);
      return false;
    }
    return true;
  }

  public Map<String, Object> getActionConfigSettings() {
    return conf.getActionConfigSettings();
  }

  public Map<String, Object> getDeciderConfigSettings() {
    return conf.getDeciderConfigSettings();
  }

  /**
   * Given the name of the tunable, it returns the BucketCalculator. The BucketCalculator is constructed
   * from the thresholds provided as the rca.conf settings. An example will look like this:
   * "bucketization": {
   *     "base1": {
   *       "UNDER_UTILIZED": 20.0,
   *       "HEALTHY_WITH_BUFFER": 40.0,
   *       "HEALTHY": 80.0
   *     },
   *     "base2": {
   *       "UNDER_UTILIZED": 30.0,
   *       "HEALTHY_WITH_BUFFER": 40.0,
   *       "HEALTHY": 75.0
   *     },
   *     "base3": {
   *       "UNDER_UTILIZED": 30.1,
   *       "HEALTHY_WITH_BUFFER": 40.23456,
   *       "HEALTHY": 75.0
   *     }
   *   }
   * @param tunableName The name of the tunable. In the above example, base1, base2, base3 are names of tunables.
   * @return The BucketCalculator that can be used to bucketize values.
   */
  public BucketCalculator getBucketizationSettings(String tunableName) {
    if (tunableResourceToUsageBucket.isEmpty()) {
      constructTunableResourceToUsageBucket();
    }
    BucketCalculator bucketCalculator = tunableResourceToUsageBucket.get(tunableName);
    if (bucketCalculator == null) {
      throw new IllegalArgumentException("No such tunable exists with name " + tunableName
          + ". Available ones: " + tunableResourceToUsageBucket.keySet());
    }
    return bucketCalculator;
  }

  private void constructTunableResourceToUsageBucket() {
    Map<String, Object> tunableSettingsMap = conf.getBucketizationTunings();
    if (tunableSettingsMap == null) {
      // The bucketization key does not exist in the rca.conf. So we cannot construct BucketCalculators.
      return;
    }
    for (Map.Entry<String, Object> entry : tunableSettingsMap.entrySet()) {
      String currentTunable = entry.getKey();
      if (entry.getValue() instanceof Map) {
        final ImmutableMap.Builder<UsageBucket, Double> usageBucketLimitMapBuilder = ImmutableMap.builder();

        Map<String, Double> bucketUpperLimitPair = (Map<String, Double>) entry.getValue();
        for (Map.Entry<String, Double> bucketUpperLimitEntry : bucketUpperLimitPair.entrySet()) {
          usageBucketLimitMapBuilder.put(
              UsageBucket.valueOf(bucketUpperLimitEntry.getKey()),
              bucketUpperLimitEntry.getValue()
          );
        }
        BucketCalculator calculator = new BasicBucketCalculator(usageBucketLimitMapBuilder.build());
        BucketCalculator old = tunableResourceToUsageBucket.put(currentTunable, calculator);
        if (old != null) {
          throw new IllegalStateException("Entry '" + currentTunable + "' exists twice." + calculator + ";" + old);
        }
      } else {
        throw new IllegalStateException(
            "Each tunable resource must be a json Object type. Not so for " + currentTunable);
      }
    }
  }
}
