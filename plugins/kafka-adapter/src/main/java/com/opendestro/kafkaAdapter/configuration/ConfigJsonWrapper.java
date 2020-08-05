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
