package com.opendestro.kafkaAdapter.starter;

import com.opendestro.kafkaAdapter.configuration.KafkaAdapterConf;
import com.opendestro.kafkaAdapter.configuration.ProducerConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.opendestro.kafkaAdapter.util.KafkaAdapterConsts;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.opendestro.kafkaAdapter.util.Helper;
import com.opendestro.kafkaAdapter.util.Target;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;


public class ProducerStarter {

    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LogManager.getLogger(ProducerStarter.class);
    public static void writeToKafkaQueue(Target target, ProducerConfiguration producerConfig){
        Timer timer = new Timer();
        KafkaProducer<String, JsonNode> producer = producerConfig.CreateProducer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try{
                    String resp = Helper.makeRequest(target);
                    JsonNode jsonNode = mapper.readTree(resp);
                    ProducerRecord<String, JsonNode> record = new ProducerRecord<String, JsonNode>(producerConfig.getTopic(), jsonNode);
                    producer.send(record);
                    producer.flush();
                } catch (JsonProcessingException e){
                    LOG.error("Exception Found On Processing Json: " + e.getMessage());
                } catch (KafkaException e){
                    LOG.error("Exception Found on Kafka: " + e.getMessage());
                    producer.close();
                } finally {
                    this.cancel();
                }
            }
        }, 0, producerConfig.getInterval());
    }

    public static void startProducer() {
        KafkaAdapterConf conf = new KafkaAdapterConf(KafkaAdapterConsts.KAFKA_ADAPTER_FILENAME);
        String bootstrapServer = conf.getKafkaBootstrapServer();
        String topic = conf.getKafkaTopicName();
        long interval = conf.getSendPeriodicityMillis();
        String params = conf.getQueryParams();

        String url = "localhost:9600/_opendistro/_performanceanalyzer/rca" + (params.equals("all") ? "" : "?name=" + params);
        Target target = new Target(url);
        ProducerConfiguration producerConfig = new ProducerConfiguration( bootstrapServer, topic, interval);
        writeToKafkaQueue(target, producerConfig);
    }
}
