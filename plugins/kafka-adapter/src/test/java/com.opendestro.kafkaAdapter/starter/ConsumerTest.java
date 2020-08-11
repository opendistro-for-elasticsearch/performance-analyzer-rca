package com.opendestro.kafkaAdapter.starter;

import com.opendestro.kafkaAdapter.configuration.ConsumerConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.opendestro.kafkaAdapter.configuration.KafkaAdapterConf;
import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import org.junit.Assert;

import java.nio.file.Paths;

public class ConsumerTest {
    private ConsumerConfiguration consumerConfig;
    @Test
    public void consumerConfigTest(){
        String kafkaAdapterConfPath = Paths.get(KafkaAdapterConsts.CONFIG_DIR_TEST_PATH, KafkaAdapterConsts.KAFKA_ADAPTER_TEST_FILENAME).toString();
        KafkaAdapterConf conf = new KafkaAdapterConf(kafkaAdapterConfPath);
        consumerConfig = new ConsumerConfiguration(conf.getKafkaBootstrapServer(), conf.getKafkaTopicName(), 5000);
        KafkaConsumer<String, JsonNode> consumer1 = consumerConfig.createConsumer();
        Assert.assertNotNull(consumer1);
        Assert.assertEquals("test_rca", consumerConfig.getTopic());
        Assert.assertEquals(KafkaAdapterConsts.KAFKA_MINIMAL_RECEIVE_PERIODICITY, consumerConfig.getInterval());
        Assert.assertEquals(KafkaAdapterConsts.DEFAULT_BOOTSTRAP_SERVER, consumerConfig.getBootstrap_server());
    }
}
