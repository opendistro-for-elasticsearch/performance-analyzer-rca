package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config;

import java.nio.file.Paths;

public class ConfConsts {

    public static final String KAFKA_BOOTSTRAP_SERVER_KEY = "bootstrap-server";

    public static final String KAFKA_TOPIC_KEY = "kafka-topic";

    public static final String WEBHOOKS_URL_KEY = "webhooks-url";

    public static final String CONFIG_DIR_PATH = Paths.get(System.getProperty("user.dir"), "pa_config").toString();

    public static final String CONFIG_DIR_TEST_PATH = Paths.get(System.getProperty("user.dir"), "src", "test", "resources").toString();

    public static final String PLUGINS_CONF_FILENAMES = "plugins.conf";

    public static final String PLUGINS_CONF_TEST_FILENAMES = "plugins_test.conf";

}

