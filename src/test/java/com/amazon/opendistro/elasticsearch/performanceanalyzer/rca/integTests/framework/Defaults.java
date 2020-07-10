/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework;

public class Defaults {
  public static final String TEST_RESOURCES_DIR = "./src/test/resources/rca/";
  public static final String RCA_CONF_DATA_NODE = TEST_RESOURCES_DIR + "rca.conf";
  public static final String RCA_CONF_ELECTED_MASTER_NODE = TEST_RESOURCES_DIR + "rca_elected_master.conf";
  public static final String RCA_CONF_STANDBY_MASTER_NODE = TEST_RESOURCES_DIR + "rca_master.conf";
}
