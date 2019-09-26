package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions;

public class MalformedAnalysisGraph extends RuntimeException {
  public MalformedAnalysisGraph(String message) {
    super(message);
  }
}
