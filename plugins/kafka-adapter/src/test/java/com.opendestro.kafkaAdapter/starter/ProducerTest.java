package com.opendestro.kafkaAdapter.starter;

import com.opendestro.kafkaAdapter.configuration.KafkaAdapterConf;
import com.opendestro.kafkaAdapter.configuration.ProducerConfiguration;
import com.opendestro.kafkaAdapter.util.Helper;
import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import com.opendestro.kafkaAdapter.util.Target;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.junit.Test;

import org.junit.Assert;

import java.nio.file.Paths;

public class ProducerTest {

    private Target target1;
    private Target target2;
    private ProducerConfiguration config1;
    private ProducerConfiguration config2;


    @Test
    public void producerConfigTest() {
        String kafkaAdapterConfPath = Paths.get(KafkaAdapterConsts.CONFIG_DIR_TEST_PATH, KafkaAdapterConsts.KAFKA_ADAPTER_TEST_FILENAME).toString();
        KafkaAdapterConf conf = new KafkaAdapterConf(kafkaAdapterConfPath);
        target1 = new Target(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT);
        target2 = new Target(KafkaAdapterConsts.PA_RCA_QUERY_ENDPOINT,"ClusterTemperatureRca");
        config1 = new ProducerConfiguration(conf.getKafkaBootstrapServer(), conf.getKafkaTopicName(), conf.getSendPeriodicityMillis());
        config2 = new ProducerConfiguration(conf.getKafkaBootstrapServer(), conf.getKafkaTopicName(),15000);
        Assert.assertEquals(KafkaAdapterConsts.KAFKA_MINIMAL_SEND_PERIODICITY, config1.getInterval());
        Assert.assertEquals(15000, config2.getInterval());
        Assert.assertEquals(KafkaAdapterConsts.DEFAULT_BOOTSTRAP_SERVER, config1.getBootstrap_server());
        KafkaProducer<String, JsonNode> producer = config1.createProducer();
        Assert.assertNotNull(producer);
        Assert.assertNotNull(Helper.makeRequest(target1));
    }
}
