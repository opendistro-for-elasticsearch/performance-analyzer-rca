package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugis.config.ConfConsts;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;


public class DecisionToKafkaPlugin extends Plugin implements ActionListener {

    public static final String NAME = "DecisionToKafkaPlugin";
    private static final Logger LOG = LogManager.getLogger(PublisherEventsLogger.class);

    public static PluginConfig pluginConfig = null;
    private static KafkaProducer<String,String> kafkaProducerInstance = null;


    @Override
    public void actionPublished(Action action) {
        LOG.info("Action: [{}] published by decision maker publisher.", action.name());
        if(pluginConfig == null){
            pluginConfig = makeSingletonPluginConfig();
        }
        if(kafkaProducerInstance == null){
            kafkaProducerInstance = makeSingletonKafkaProducer();
        }
        String summary = action.summary();
    sendSummaryToKafkaQueue(summary);
    }

    private PluginConfig makeSingletonPluginConfig() {
        return new PluginConfig(ConfConsts.PLUGIN_CONF_FILENAME);
    }

    // TODO: make kafka producer shared by multiple plugins
    public KafkaProducer<String, String> makeSingletonKafkaProducer(){
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_BOOTSTRAP_SERVER_KEY));
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(configProperties);
    }

    public void sendSummaryToKafkaQueue(String msg){
        try{
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_TOPIC_KEY), msg);
            kafkaProducerInstance.send(record);
        } catch (KafkaException e){
            LOG.error("Exception Found on Kafka: " + e.getMessage());
        }
        kafkaProducerInstance.close();
    }

    @Override
    public String name() {
        return NAME;
    }

}
