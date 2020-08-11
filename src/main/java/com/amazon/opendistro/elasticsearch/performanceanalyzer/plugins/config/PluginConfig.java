package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class PluginConfig {
    protected String configFilePath;
    protected PluginConfJsonWrapper conf;
    private static final Logger LOG = LogManager.getLogger(PluginConfig.class);

    public PluginConfig(String configPath) {
        this.configFilePath = configPath;
        JsonFactory factory = new JsonFactory();
        factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
        ObjectMapper mapper = new ObjectMapper(factory);
        try {
            File configFile = new File(this.configFilePath);
            this.conf = mapper.readValue(configFile, PluginConfJsonWrapper.class);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    //for test
    public PluginConfig(PluginConfJsonWrapper conf){
        this.conf = conf;
    }

    public Map<String, String> getKafkaDecisionListenerSettings(){
        return conf.getKafkaDecisionListenerSettings();
    }

    public String getKafkaDecisionListenerConfig(String key){
        return getKafkaDecisionListenerSettings().get(key);
    }
}

