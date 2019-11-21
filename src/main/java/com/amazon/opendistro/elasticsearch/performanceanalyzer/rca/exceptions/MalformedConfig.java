package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions;

public class MalformedConfig extends Exception {
  public MalformedConfig(String fileLocation, String errorMessage, Throwable err) {
    super(String.format("Malformed config file: (%s). Err: %s", fileLocation, errorMessage), err);
  }

  public MalformedConfig(String fileLocation, String errorMessage) {
    super(String.format("Malformed config file: (%s). Err: %s", fileLocation, errorMessage));
  }
}
