package com.opendestro.kafkaAdapter.util;

import java.nio.file.Paths;

public class KafkaAdapterConsts {
    public static final String CONFIG_DIR_PATH = Paths.get(System.getProperty("user.dir"), "config").toString();

    public static final String CONFIG_DIR_TEST_PATH = Paths.get(System.getProperty("user.dir"), "src", "test", "resources").toString();

    public static final String KAFKA_ADAPTER_FILENAME = "kafka_adapter.conf";

    public static final String KAFKA_ADAPTER_TEST_FILENAME = "kafka_adapter_test.conf";

    public static final String PA_RCA_QUERY_ENDPOINT = "localhost:9600/_opendistro/_performanceanalyzer/rca";

    public static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";

    public static final long KAFKA_MINIMAL_SEND_PERIODICITY = 10000;

    public static final long KAFKA_MINIMAL_RECEIVE_PERIODICITY = 10000;
}
