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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import java.nio.file.Paths;

public class RcaConsts {

  public static final String RCA_CONF_MASTER_FILENAME = "rca_master.conf";
  private static final String RCA_CONF_FILENAME = "rca.conf";
  private static final String RCA_CONF_IDLE_MASTER_FILENAME = "rca_idle_master.conf";
  private static final String THRESHOLDS_DIR_NAME = "thresholds";
  private static final String CONFIG_DIR_PATH =
      Paths.get(Util.READER_LOCATION, "pa_config").toString();
  public static final String RCA_CONF_PATH =
      Paths.get(CONFIG_DIR_PATH, RCA_CONF_FILENAME).toString();
  public static final String RCA_CONF_MASTER_PATH =
      Paths.get(CONFIG_DIR_PATH, RCA_CONF_MASTER_FILENAME).toString();
  public static final String RCA_CONF_IDLE_MASTER_PATH =
      Paths.get(CONFIG_DIR_PATH, RCA_CONF_IDLE_MASTER_FILENAME).toString();
  public static final String THRESHOLDS_PATH =
      Paths.get(CONFIG_DIR_PATH, THRESHOLDS_DIR_NAME).toString();

  static final String dir = System.getProperty("user.dir");
  public static final String TEST_CONFIG_PATH =
      Paths.get(dir, "src", "test", "resources", "rca").toString();
  // Paths.get(dir, "build", "resources", "test", "rca").toString();

  // These are some of the constants that will be expected in the rca.conf file.
  public static final String DATASTORE_TYPE_KEY = "type";
  public static final String DATASTORE_LOC_KEY = "location-dir";
  public static final String DATASTORE_FILENAME = "filename";
  public static final String DATASTORE_TIMESTAMP_COL_NAME = "timestamp";
  public static final String DATASTORE_RESOURCE_COL_NAME = "resource";
  public static final String DATASTORE_STATE_COL_NAME = "state";
}
