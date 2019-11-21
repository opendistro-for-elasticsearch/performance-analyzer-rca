package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions;

public class OverridesAndPrecedenceOrderValueMismatch extends MalformedThresholdFile {
  public OverridesAndPrecedenceOrderValueMismatch(
      String fileLocation, String errorMessage, Throwable err) {
    super(fileLocation, errorMessage, err);
  }

  public OverridesAndPrecedenceOrderValueMismatch(String fileLocation, String errorMessage) {
    super(fileLocation, errorMessage);
  }
}
