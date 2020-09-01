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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PluginConfJsonWrapper {
    private final Map<String, String> kafkaDecisionListenerSettings;
    private final Map<String, String> kafkaClusterRcaListenerSettings;

    public Map<String, String> getKafkaDecisionListenerSettings() {
        return kafkaDecisionListenerSettings;
    }

    public Map<String, String> getKafkaClusterRcaListenerSettings() {
        return kafkaClusterRcaListenerSettings;
    }

    PluginConfJsonWrapper(
            @JsonProperty("kafka-decision-listener") Map<String, String> kafkaDecisionListenerSettings,
            @JsonProperty("kafka-cluster-rca-listener") Map<String, String> kafkaClusterRcaListenerSettings) {
        this.kafkaDecisionListenerSettings = kafkaDecisionListenerSettings;
        this.kafkaClusterRcaListenerSettings = kafkaClusterRcaListenerSettings;
    }
}