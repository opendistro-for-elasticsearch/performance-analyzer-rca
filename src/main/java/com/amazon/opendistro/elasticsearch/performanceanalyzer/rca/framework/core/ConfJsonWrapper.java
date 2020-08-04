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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO: There should be a validation for the expected fields.
@JsonIgnoreProperties(ignoreUnknown = true)
class ConfJsonWrapper {

  private static final Logger LOG = LogManager.getLogger(ConfJsonWrapper.class);
  private final String rcaStoreLoc;
  private final String thresholdStoreLoc;
  private final long newRcaCheckPeriocicityMins;
  private final long newThresholdCheckPeriodicityMins;
  private final List<String> peerIpList;
  private final Map<String, String> tagMap;
  private final long creationTime;
  private final Map<String, String> datastore;
  private final String analysisGraphEntryPoint;
  private final int networkQueueLength;
  private final int perVertexBufferLength;
  private final Map<String, Object> rcaConfigSettings;
  private final List<String> mutedRcaList;
  private final Map<String, Object> deciderConfigSettings;

  String getRcaStoreLoc() {
    return rcaStoreLoc;
  }

  String getThresholdStoreLoc() {
    return thresholdStoreLoc;
  }

  long getNewRcaCheckPeriocicityMins() {
    return newRcaCheckPeriocicityMins;
  }

  long getNewThresholdCheckPeriodicityMins() {
    return newThresholdCheckPeriodicityMins;
  }

  List<String> getPeerIpList() {
    return peerIpList;
  }

  Map<String, String> getTagMap() {
    return tagMap;
  }

  Map<String, String> getDatastore() {
    return Collections.unmodifiableMap(datastore);
  }

  String getAnalysisGraphEntryPoint() {
    return analysisGraphEntryPoint;
  }

  int getNetworkQueueLength() {
    return networkQueueLength;
  }

  int getPerVertexBufferLength() {
    return perVertexBufferLength;
  }

  List<String> getMutedRcaList() {
    return mutedRcaList;
  }

  public void setDatastoreRcaLogDirectory(String rcaLogLocation) {
    this.datastore.put(RcaConsts.DATASTORE_LOC_KEY, rcaLogLocation);
  }

  Map<String, Object> getRcaConfigSettings() {
    return rcaConfigSettings;
  }

  Map<String, Object> getDeciderConfigSettings() {
    return deciderConfigSettings;
  }

  ConfJsonWrapper(
      @JsonProperty("rca-store-location") String rcaStoreLoc,
      @JsonProperty("threshold-store-location") String thresholdStoreLoc,
      @JsonProperty("new-rca-check-minutes") long newRcaCheckPeriocicityMins,
      @JsonProperty("new-threshold-check-minutes") long newThresholdCheckPeriodicityMins,
      @JsonProperty("tags") Map<String, String> tags,
      @JsonProperty("remote-peers") List<String> peers,
      @JsonProperty("datastore") Map<String, String> datastore,
      @JsonProperty("analysis-graph-implementor") String analysisGraphEntryPoint,
      @JsonProperty("network-queue-length") int networkQueueLength,
      @JsonProperty("max-flow-units-per-vertex-buffer") int perVertexBufferLength,
      @JsonProperty("rca-config-settings") Map<String, Object> rcaConfigSettings,
      @JsonProperty("muted-rcas") List<String> mutedRcas,
      @JsonProperty("decider-config-settings") Map<String, Object> deciderConfigSettings) {
    this.creationTime = System.currentTimeMillis();
    this.rcaStoreLoc = rcaStoreLoc;
    this.thresholdStoreLoc = thresholdStoreLoc;
    this.newRcaCheckPeriocicityMins = newRcaCheckPeriocicityMins;
    this.newThresholdCheckPeriodicityMins = newThresholdCheckPeriodicityMins;
    this.peerIpList = peers;
    this.tagMap = tags;
    this.datastore = datastore;
    this.analysisGraphEntryPoint = analysisGraphEntryPoint;
    this.networkQueueLength = networkQueueLength;
    this.perVertexBufferLength = perVertexBufferLength;
    this.rcaConfigSettings = rcaConfigSettings;
    this.mutedRcaList = mutedRcas;
    this.deciderConfigSettings = deciderConfigSettings;
  }
}
