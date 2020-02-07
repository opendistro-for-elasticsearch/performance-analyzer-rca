package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;

import java.io.File;

public class AESRcaConf extends RcaConf {

  private final String aesRcaLogLocation =
      Util.ES_HOME + File.separator + "var" + File.separator + "output" + File.separator + "logs";

  public AESRcaConf(String configPath) {
    super(configPath);
    // This change is for AES, where the RCA log directory will be changed to ES_HOME/var/output/log
    // for open source, it will be as provided in rca config file
    this.conf.setDatastoreRcaLogDirectory(aesRcaLogLocation);
  }
}