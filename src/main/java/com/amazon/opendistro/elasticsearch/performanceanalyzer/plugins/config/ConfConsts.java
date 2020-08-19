/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config;

import java.nio.file.Paths;

public class ConfConsts {

    public static final String KAFKA_BOOTSTRAP_SERVER_KEY = "bootstrap-server";

    public static final String DECISION_KAFKA_TOPIC_KEY = "decision-kafka-topic";

    public static final String CLUSTER_SUMMARY_KAFKA_TOPIC_KEY = "cluster-summary-kafka-topic";

    public static final String WEBHOOKS_URL_KEY = "webhooks-url";

    public static final String CONFIG_DIR_PATH = Paths.get(System.getProperty("user.dir"), "pa_config").toString();

    public static final String CONFIG_DIR_TEST_PATH = Paths.get(System.getProperty("user.dir"), "src", "test", "resources").toString();

    public static final String PLUGINS_CONF_FILENAMES = "plugins.conf";

    public static final String PLUGINS_CONF_TEST_FILENAMES = "plugins_test.conf";

}

