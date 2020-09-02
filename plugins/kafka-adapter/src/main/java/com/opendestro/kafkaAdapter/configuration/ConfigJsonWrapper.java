/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
class ConfigJsonWrapper {
    private final String kafkaBootstrapServer;
    private final String kafkaTopic;
    private final String webhooksUrl;
    private final long receivePeriodicityMillis;
    private final int maxNoMessageFoundCountOnConsumer;

    public int getMaxNoMessageFoundCountOnConsumer() {
        return maxNoMessageFoundCountOnConsumer;
    }

    public long getReceivePeriodicityMillis() {
        return receivePeriodicityMillis;
    }

    public String getKafkaBootstrapServer() {
        return kafkaBootstrapServer;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getWebhooksUrl() {
        return webhooksUrl;
    }

    ConfigJsonWrapper(
            @JsonProperty("bootstrap_server") String kafkaBootstrapServer,
            @JsonProperty("kafka_topic") String topic,
            @JsonProperty("webhooks_url") String webhooksUrl,
            @JsonProperty("kafka_consumer_interval") long receivePeriodicityMillis,
            @JsonProperty("max_no_message_found_count") int maxNoMessageFoundCountOnConsumer) {
        this.kafkaBootstrapServer = kafkaBootstrapServer;
        this.kafkaTopic = topic;
        this.webhooksUrl = webhooksUrl;
        this.receivePeriodicityMillis = receivePeriodicityMillis;
        this.maxNoMessageFoundCountOnConsumer = maxNoMessageFoundCountOnConsumer;
    }
}
