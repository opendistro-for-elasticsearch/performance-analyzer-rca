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
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// TODO: There should be a validation for the expected fields.
@JsonIgnoreProperties(ignoreUnknown = true)
class ConfJsonWrapper {
  public static final String RCA_STORE_LOC = "rca-store-location";
  public static final String THRESHOLD_STORE_LOC = "threshold-store-location";
  public static final String NEW_RCA_CHECK_MINS = "new-rca-check-minutes";
  public static final String NEW_THRESHOLDS_CHECK_MINS = "new-threshold-check-minutes";
  public static final String TAGS = "tags";
  public static final String REMOTE_PEERS = "remote-peers";
  public static final String DATASTORE = "datastore";
  public static final String ANALYSIS_GRAPH_IMPL = "analysis-graph-implementor";
  public static final String NETWORK_QUEUE_LEN = "network-queue-length";
  public static final String MAX_FLOW_UNIT_PER_VERTEX = "max-flow-units-per-vertex-buffer";
  public static final String RCA_CONFIG_SETTINGS = "rca-config-settings";
  public static final String MUTED_RCAS = "muted-rcas";
  public static final String MUTED_DECIDERS = "muted-deciders";
  public static final String MUTED_ACTIONS = "muted-actions";
  public static final String DECIDER_CONFIG_SETTINGS = "decider-config-settings";
  public static final String USAGE_BUCKET_THRESHOLDS = "usage-bucket-thresholds";

  private final String rcaStoreLoc;
  private final String thresholdStoreLoc;
  private final long newRcaCheckPeriodicityMins;
  private final long newThresholdCheckPeriodicityMins;
  private final List<String> peerIpList;
  private final Map<String, String> tagMap;
  private final long creationTime;
  private final Map<String, String> datastore;
  private final String analysisGraphEntryPoint;
  private final int networkQueueLength;
  private final int perVertexBufferLength;
  private final Map<String, Object> rcaConfigSettings;
  private final Map<String, Object> deciderConfigSettings;
  private final List<String> mutedRcaList;
  private final List<String> mutedDeciderList;
  private final List<String> mutedActionList;
  private final Map<String, List<Double>> usageBucketThresholds;

  String getRcaStoreLoc() {
    return rcaStoreLoc;
  }

  String getThresholdStoreLoc() {
    return thresholdStoreLoc;
  }

  long getNewRcaCheckPeriodicityMins() {
    return newRcaCheckPeriodicityMins;
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

  public List<String> getMutedDeciderList() {
    return mutedDeciderList;
  }

  public List<String> getMutedActionList() {
    return mutedActionList;
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

  Map<String, List<Double>> getUsageBucketThresholds() {
    return usageBucketThresholds;
  }

  ConfJsonWrapper(
      @JsonProperty(RCA_STORE_LOC) String rcaStoreLoc,
      @JsonProperty(THRESHOLD_STORE_LOC) String thresholdStoreLoc,
      @JsonProperty(NEW_RCA_CHECK_MINS) long newRcaCheckPeriodicityMins,
      @JsonProperty(NEW_THRESHOLDS_CHECK_MINS) long newThresholdCheckPeriodicityMins,
      @JsonProperty(TAGS) Map<String, String> tags,
      @JsonProperty(REMOTE_PEERS) List<String> peers,
      @JsonProperty(DATASTORE) Map<String, String> datastore,
      @JsonProperty(ANALYSIS_GRAPH_IMPL) String analysisGraphEntryPoint,
      @JsonProperty(NETWORK_QUEUE_LEN) int networkQueueLength,
      @JsonProperty(MAX_FLOW_UNIT_PER_VERTEX) int perVertexBufferLength,
      @JsonProperty(RCA_CONFIG_SETTINGS) Map<String, Object> rcaConfigSettings,
      @JsonProperty(MUTED_RCAS) List<String> mutedRcas,
      @JsonProperty(MUTED_DECIDERS) List<String> mutedDeciders,
      @JsonProperty(MUTED_ACTIONS) List<String> mutedActions,
      @JsonProperty(DECIDER_CONFIG_SETTINGS) Map<String, Object> deciderConfigSettings,
      @JsonProperty(USAGE_BUCKET_THRESHOLDS) Map<String, List<Double>> usageBucketThresholds) {
    this.creationTime = System.currentTimeMillis();
    this.rcaStoreLoc = rcaStoreLoc;
    this.thresholdStoreLoc = thresholdStoreLoc;
    this.newRcaCheckPeriodicityMins = newRcaCheckPeriodicityMins;
    this.newThresholdCheckPeriodicityMins = newThresholdCheckPeriodicityMins;
    this.peerIpList = peers;
    this.tagMap = tags;
    this.datastore = datastore;
    this.analysisGraphEntryPoint = analysisGraphEntryPoint;
    this.networkQueueLength = networkQueueLength;
    this.perVertexBufferLength = perVertexBufferLength;
    this.rcaConfigSettings = rcaConfigSettings;
    this.mutedRcaList = ImmutableList.copyOf(mutedRcas);
    this.mutedDeciderList = ImmutableList.copyOf(mutedDeciders);
    this.mutedActionList = ImmutableList.copyOf(mutedActions);
    this.deciderConfigSettings = deciderConfigSettings;
    this.usageBucketThresholds = usageBucketThresholds;
  }
}
