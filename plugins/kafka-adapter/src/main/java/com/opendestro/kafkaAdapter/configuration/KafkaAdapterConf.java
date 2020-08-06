package com.opendestro.kafkaAdapter.configuration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class KafkaAdapterConf {
    private String configFileLoc;
    private ConfigJsonWrapper conf;

    public KafkaAdapterConf (String configPath){
        this.configFileLoc = configPath;
        JsonFactory factory = new JsonFactory();
        factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
        ObjectMapper mapper = new ObjectMapper(factory);
        try{
            File configFile = new File(this.configFileLoc);
            this.conf = mapper.readValue(configFile, ConfigJsonWrapper.class);
        }catch(IOException e){
            System.out.println("error");
        }
    }

    public String getConfigFileLoc() {
        return configFileLoc;
    }

    public int getMaxNoMessageFoundCountOnConsumer() {
        return conf.getMaxNoMessageFoundCountOnConsumer();
    }

    public long getSendPeriodicityMillis() {
        return conf.getSendPeriodicityMillis();
    }

    public long getReceivePeriodicityMillis() {
        return conf.getReceivePeriodicityMillis();
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
