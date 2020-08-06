package com.opendestro.kafkaAdapter.configuration;

import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

public class ConfTest{

    @Test
    public void testKafkaAdapterConfig(){
        String kafkaAdapterConfPath = Paths.get(KafkaAdapterConsts.CONFIG_DIR_TEST_PATH, KafkaAdapterConsts.KAFKA_ADAPTER_TEST_FILENAME).toString();
        KafkaAdapterConf conf = new KafkaAdapterConf(kafkaAdapterConfPath);
        Assert.assertEquals( kafkaAdapterConfPath, conf.getConfigFileLoc());
        Assert.assertEquals("localhost:9092", conf.getKafkaBootstrapServer());
        Assert.assertEquals("test_rca", conf.getKafkaTopicName());
        Assert.assertEquals(2000, conf.getSendPeriodicityMillis());
        Assert.assertEquals(20000, conf.getReceivePeriodicityMillis());
        Assert.assertEquals("all", conf.getQueryParams());
        Assert.assertEquals(3, conf.getMaxNoMessageFoundCountOnConsumer());
    }
}
