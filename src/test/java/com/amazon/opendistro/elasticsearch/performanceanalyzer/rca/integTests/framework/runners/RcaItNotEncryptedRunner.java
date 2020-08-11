package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners;

public class RcaItNotEncryptedRunner extends RcaItRunnerBase {
  private static final boolean USE_HTTPS = false;

  public RcaItNotEncryptedRunner(Class testClass) throws Exception {
    super(testClass, USE_HTTPS);
  }
}
