package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions;

public class MalformedThresholdFile extends MalformedConfig {
  public MalformedThresholdFile(String fileLocation, String errorMessage, Throwable err) {
    super(fileLocation, errorMessage, err);
  }

  public MalformedThresholdFile(String fileLocation, String errorMessage) {
    super(fileLocation, errorMessage);
  }
}
