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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
class ConfigJsonWrapper {
    private final String kafkaBootstrapServer;
    private final String kafkaTopic;
    private final String queryParams;
    private final String webhooksUrl;
    private final long sendPeriodicityMillis;
    private final long receivePeriodicityMillis;
    private final int maxNoMessageFoundCountOnConsumer;

    public int getMaxNoMessageFoundCountOnConsumer() {
        return maxNoMessageFoundCountOnConsumer;
    }

    public long getReceivePeriodicityMillis() {
        return receivePeriodicityMillis;
    }

    public long getSendPeriodicityMillis() {
        return sendPeriodicityMillis;
    }

    public String getKafkaBootstrapServer() {
        return kafkaBootstrapServer;
    }

    public String getKafkaTopicName() {
        return kafkaTopic;
    }

    public String getQueryParams() {
        return queryParams;
    }

    public String getWebhooksUrl() {
        return webhooksUrl;
    }

    ConfigJsonWrapper(
            @JsonProperty("bootstrap_server") String kafkaBootstrapServer,
            @JsonProperty("topic") String kafkaTopic,
            @JsonProperty("params") String queryParams,
            @JsonProperty("webhooks_url") String webhooksUrl,
            @JsonProperty("kafka_producer_interval") long sendPeriodicityMillis,
            @JsonProperty("kafka_consumer_interval") long receivePeriodicityMillis,
            @JsonProperty("max_no_message_found_count") int maxNoMessageFoundCountOnConsumer){
        this.kafkaBootstrapServer = kafkaBootstrapServer;
        this.kafkaTopic = kafkaTopic;
        this.queryParams = queryParams;
        this.webhooksUrl = webhooksUrl;
        this.sendPeriodicityMillis = sendPeriodicityMillis;
        this.receivePeriodicityMillis = receivePeriodicityMillis;
        this.maxNoMessageFoundCountOnConsumer = maxNoMessageFoundCountOnConsumer;
    }
}
