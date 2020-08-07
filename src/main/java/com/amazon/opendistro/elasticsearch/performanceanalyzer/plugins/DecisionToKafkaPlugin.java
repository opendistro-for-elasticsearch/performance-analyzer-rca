package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.ConfConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.ConfConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;
import java.util.Properties;


public class DecisionToKafkaPlugin extends Plugin implements ActionListener {

    public static final String NAME = "DecisionToKafkaPlugin";
    private static final Logger LOG = LogManager.getLogger(DecisionToKafkaPlugin.class);

    public static PluginConfig pluginConfig = null;
    private static KafkaProducer<String,String> kafkaProducerInstance = null;


    @Override
    public void actionPublished(Action action) {
        LOG.info("Action: [{}] published by decision maker publisher.", action.name());
        if(pluginConfig == null){
            pluginConfig = makeSingletonPluginConfig();
        }
        System.out.println("value: " + pluginConfig.getKafkaDecisionListenerConfig("kafka-topic"));
        if(kafkaProducerInstance == null){
            kafkaProducerInstance = makeSingletonKafkaProducer();
        }
        String summary = action.summary();
        sendSummaryToKafkaQueue(summary);
    }

    private PluginConfig makeSingletonPluginConfig() {
        String pluginConfPath = Paths.get(ConfConsts.CONFIG_DIR_PATH, ConfConsts.DECISION_LISTENER_CONF_FILENAME).toString();
        System.out.println("Plugin Path: "+pluginConfPath);
        return new PluginConfig(pluginConfPath);
    }

    public KafkaProducer<String, String> makeSingletonKafkaProducer(){
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_BOOTSTRAP_SERVER_KEY));
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(configProperties);
    }

    public void sendSummaryToKafkaQueue(String msg){
        try{
            String kafkaTopic = pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_TOPIC_KEY);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, msg);
            LOG.info(String.format("sending record: %s to kafka topic: %s ", msg, kafkaTopic));
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

    public static void main(String[] args) {
        // TEST
        NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"),
                new InstanceDetails.Ip("1.2.3.4"));
        ModifyCacheCapacityAction modifyCacheCapacityAction =
                new ModifyCacheCapacityAction(node1, ResourceEnum.FIELD_DATA_CACHE, 5000, true);
        DecisionToKafkaPlugin plugin = new DecisionToKafkaPlugin();
        plugin.actionPublished(modifyCacheCapacityAction);
    }

}
