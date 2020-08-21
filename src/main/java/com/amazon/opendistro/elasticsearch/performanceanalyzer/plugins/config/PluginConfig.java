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

