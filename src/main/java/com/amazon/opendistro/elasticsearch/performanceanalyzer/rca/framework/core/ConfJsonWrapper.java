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
}
