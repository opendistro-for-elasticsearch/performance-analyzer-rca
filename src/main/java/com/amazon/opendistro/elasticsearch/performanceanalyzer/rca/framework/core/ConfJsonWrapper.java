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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// TODO: There should be a validation for the expected fields.
@JsonIgnoreProperties(ignoreUnknown = true)
class ConfJsonWrapper {
  private final String rcaStoreLoc;
  private final String thresholdStoreLoc;
  private final long newRcaCheckPeriocicityMins;
  private final long newThresholdCheckPeriodicityMins;
  private final List<String> peerIpList;
  private final Map<String, String> tagMap;
  private final long creationTime;
  private final Map<String, String> datastore;
  private final String analysisGraphEntryPoint;

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

  ConfJsonWrapper(
      @JsonProperty("rca-store-location") String rcaStoreLoc,
      @JsonProperty("threshold-store-location") String thresholdStoreLoc,
      @JsonProperty("new-rca-check-minutes") long newRcaCheckPeriocicityMins,
      @JsonProperty("new-threshold-check-minutes") long newThresholdCheckPeriodicityMins,
      @JsonProperty("tags") Map<String, String> tags,
      @JsonProperty("remote-peers") List<String> peers,
      @JsonProperty("datastore") Map<String, String> datastore,
      @JsonProperty("analysis-graph-implementor") String analysisGraphEntryPoint) {
    this.creationTime = System.currentTimeMillis();
    this.rcaStoreLoc = rcaStoreLoc;
    this.thresholdStoreLoc = thresholdStoreLoc;
    this.newRcaCheckPeriocicityMins = newRcaCheckPeriocicityMins;
    this.newThresholdCheckPeriodicityMins = newThresholdCheckPeriodicityMins;
    this.peerIpList = peers;
    this.tagMap = tags;
    this.datastore = datastore;
    this.analysisGraphEntryPoint = analysisGraphEntryPoint;
  }
}
