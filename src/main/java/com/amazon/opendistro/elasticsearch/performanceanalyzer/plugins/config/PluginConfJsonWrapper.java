package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PluginConfJsonWrapper {
    private final Map<String, String> kafkaDecisionListenerSettings;
    private static final Logger LOG = LogManager.getLogger(PluginConfJsonWrapper.class);

    public Map<String, String> getKafkaDecisionListenerSettings() {
        return kafkaDecisionListenerSettings;
    }

    PluginConfJsonWrapper(
            @JsonProperty("kafka-decision-listener") Map<String, String> kafkaDecisionListenerSettings) {
        this.kafkaDecisionListenerSettings = kafkaDecisionListenerSettings;
    }
}