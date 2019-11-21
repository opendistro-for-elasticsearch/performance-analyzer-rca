package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions;

public class OverridesAndPrecedenceOrderCountMismatch extends MalformedThresholdFile {
  public OverridesAndPrecedenceOrderCountMismatch(
      String fileLocation, String errorMessage, Throwable err) {
    super(fileLocation, errorMessage, err);
  }

  public OverridesAndPrecedenceOrderCountMismatch(String fileLocation, String errorMessage) {
    super(fileLocation, errorMessage);
  }
}
