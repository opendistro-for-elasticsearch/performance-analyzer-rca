package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import java.nio.file.Paths;

public class RcaConsts {

  // These are some of the constants that will be expected in the rca.conf file.
  public static final String DATASTORE_TYPE_KEY = "type";
  public static final String DATASTORE_LOC_KEY = "location-dir";
  public static final String DATASTORE_FILENAME = "filename";
  public static final String DATASTORE_CONTEXT_COL_NAME = "context";
  public static final String DATASTORE_TIMESTAMP_COL_NAME = "timestamp";
  static final String dir = System.getProperty("user.dir");
  public static final String TEST_CONFIG_PATH =
      Paths.get(dir, "src", "test", "resources", "rca").toString();
  // Paths.get(dir, "build", "resources", "test", "rca").toString();
  private static final String RCA_CONF_FILENAME = "rca.conf";
  private static final String THRESHOLDS_DIR_NAME = "thresholds";
  private static final String CONFIG_DIR_PATH =
      Paths.get(Util.PLUGIN_LOCATION, "pa_config").toString();
  public static final String RCA_CONF_PATH =
      Paths.get(CONFIG_DIR_PATH, RCA_CONF_FILENAME).toString();
  public static final String THRESHOLDS_PATH =
      Paths.get(CONFIG_DIR_PATH, THRESHOLDS_DIR_NAME).toString();
}
