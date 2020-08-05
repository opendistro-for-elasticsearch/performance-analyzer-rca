package com.opendestro.kafkaAdapter.configuration;

import com.opendestro.kafkaAdapter.starter.Main;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class KafkaAdapterConf {
    private String configFileLoc;
    private long lastModifiedTime;
    private ConfigJsonWrapper conf;
    private static final Logger LOG = LogManager.getLogger(KafkaAdapterConf.class);

    public KafkaAdapterConf (String configPath){
        this.configFileLoc = configPath;
        JsonFactory factory = new JsonFactory();
        factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
        ObjectMapper mapper = new ObjectMapper(factory);
        try{
            File configFile = new File(this.configFileLoc);
            this.lastModifiedTime = configFile.lastModified();
            this.conf = mapper.readValue(configFile, ConfigJsonWrapper.class);
        }catch(IOException e){
            LOG.error(e.getMessage());
        }
    }

    // for Tests
    public KafkaAdapterConf(){}

    public String getConfigFileLoc() {
        return configFileLoc;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public ConfigJsonWrapper getConf() {
        return conf;
    }

    public int getMaxNoMessageFoundCountOnConsumer() {
        return conf.getMaxNoMessageFoundCountOnConsumer();
    }

    public long getReceivePeriodicityMillis() {
        return conf.getReceivePeriodicityMillis();
    }

    public long getSendPeriodicityMillis() {
        return conf.getSendPeriodicityMillis();
    }

    public String getKafkaBootstrapServer() {
        return conf.getKafkaBootstrapServer();
    }

    public String getKafkaTopicName() {
        return conf.getKafkaTopicName();
    }

    public String getQueryParams() {
        return conf.getQueryParams();
    }

    public String getWebhooksUrl() {
        return conf.getWebhooksUrl();
    }
}
