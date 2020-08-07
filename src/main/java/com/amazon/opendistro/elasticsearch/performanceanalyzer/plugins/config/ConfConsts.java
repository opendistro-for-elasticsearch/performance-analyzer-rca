package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config;

import java.nio.file.Paths;

public class ConfConsts {
    // The configuration file name
    public static final String PLUGIN_CONF_FILENAME = "plugins.conf";

    // Key to look for in configuration file
    public static final String KAFKA_CONF_KEY = "kafka-decision-listener";

    public static final String KAFKA_BOOTSTRAP_SERVER_KEY = "bootstrap-server";

    public static final String KAFKA_TOPIC_KEY = "kafka-topic";

    public static final String WEBHOOKS_URL_KEY = "kafka-topic";

    public static final String CONFIG_DIR_PATH = Paths.get(System.getProperty("user.dir"), "pa_config").toString();

    public static final String DECISION_LISTENER_CONF_FILENAME = "plugins.conf";

}

