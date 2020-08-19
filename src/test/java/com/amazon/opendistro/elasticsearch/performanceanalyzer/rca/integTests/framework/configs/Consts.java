package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs;

public class Consts {
  public static final String RCA_IT_BASE_DIR = "/tmp/rcaIt";

  // This format is used to create a directory that will be used by the simulated host for the current test run.
  public static final String RCA_IT_CLUSTER_DIR_FORMAT = "yyyy.MM.dd.HH.mm.ss";
  public static final String TEST_RESOURCES_DIR = "./src/test/resources/rca/";
  public static final String RCAIT_DEFAULT_RCA_CONF_ELECTED_MASTER_NODE = TEST_RESOURCES_DIR + "rca_elected_master.conf";
  public static final String RCAIT_DEFAULT_RCA_CONF_STANDBY_MASTER_NODE = TEST_RESOURCES_DIR + "rca_master.conf";
  public static final String RCAIT_DEFAULT_RCA_CONF_DATA_NODE = TEST_RESOURCES_DIR + "rca.conf";

  public static final String INTEG_TESTS_SRC_DIR =
      "./src/test/java/com/amazon/opendistro/elasticsearch/performanceanalyzer/rca/integTests/";

  public static final String HOST_ID_KEY = "hostId";
  public static final String HOST_ROLE_KEY = "hostRole";
  public static final String DATA_KEY = "data";

  // Node count constants.
  public static final int numDataNodes = 2;
  public static final int numStandbyMasterNodes = 2;
}
