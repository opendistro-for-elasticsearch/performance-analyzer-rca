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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageOldGenRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageYoungGenRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotNodeClusterRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotShardClusterRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotShardRcaConfig;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RcaConf {
  protected String configFileLoc;
  protected long lastModifiedTime;
  protected ConfJsonWrapper conf;

  protected static RcaConf instance;
  private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerApp.class);

  public RcaConf(String configPath) {
    this.configFileLoc = configPath;
    JsonFactory factory = new JsonFactory();
    factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
    ObjectMapper mapper = new ObjectMapper(factory);
    mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    try {
      File configFile = new File(this.configFileLoc);
      this.lastModifiedTime = configFile.lastModified();
      this.conf = mapper.readValue(configFile, ConfJsonWrapper.class);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  // This should only be used for Tests.
  public RcaConf() {}

  public static void clear() {
    instance = null;
  }

  public String getRcaStoreLoc() {
    return conf.getRcaStoreLoc();
  }

  public String getThresholdStoreLoc() {
    return conf.getThresholdStoreLoc();
  }

  public long getNewRcaCheckPeriocicityMins() {
    return conf.getNewRcaCheckPeriocicityMins();
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

  public HighHeapUsageOldGenRcaConfig getHighHeapUsageOldGenRcaConfig() {
    return new HighHeapUsageOldGenRcaConfig(this);
  }

  public HighHeapUsageYoungGenRcaConfig getHighHeapUsageYoungGenRcaConfig() {
    return new HighHeapUsageYoungGenRcaConfig(this);
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

  public List<String> getMutedRcaList() {
    return conf.getMutedRcaList();
  }

  @VisibleForTesting
  public void setMutedRcaList(List<String> mutedRcaList) {
    conf.setMutedRcaList(mutedRcaList);
  }

  @SuppressWarnings("unchecked")
  public <T> T readRcaConfig(String rcaName, String key, Class<? extends T> clazz) {
    T setting = null;
    try {
      Map<String, Object> rcaObj = null;
      if (conf.getRcaConfigSettings() != null
          && conf.getRcaConfigSettings().containsKey(rcaName)
          && conf.getRcaConfigSettings().get(rcaName) != null) {
        rcaObj = (Map<String, Object>)conf.getRcaConfigSettings().get(rcaName);
      }

      if (rcaObj != null
          && rcaObj.containsKey(key)
          && rcaObj.get(key) != null) {
        setting = clazz.cast(rcaObj.get(key));
      }
    }
    catch (ClassCastException ne) {
      LOG.error("rca.conf contains value in invalid format, trace : {}", ne.getMessage());
    }
    return setting;
  }
}
