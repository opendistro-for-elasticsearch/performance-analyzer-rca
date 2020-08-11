/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
