package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.cluster_rca_publisher.ClusterSummaryKafkaPublisher;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.decision_publisher.DecisionKafkaPublisher;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummary;
import com.google.gson.JsonObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;


public class KafkaPublisherTest<T extends GenericSummary> {
  private static KafkaProducer<String, String> kafkaProducer;
  private static KafkaConsumer<String, String> kafkaConsumer;
  private static String testBootstrapServer = "localhost:9092";
  private static String testActionSummaryTopic = "test_action";
  private static String testClusterSummaryTopic = "test_cluster";
  private DecisionKafkaPublisher decisionKafkaPublisher;
  private ClusterSummaryKafkaPublisher clusterSummaryKafkaPublisher;

  @Mock
  private KafkaProducerController controller;

  @Mock
  private PluginConfig pluginConfig;

  @Mock
  private Action action;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    //setup sample kafka producer
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBootstrapServer);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProducer = new KafkaProducer<>(producerProps);

    Mockito.when(controller.getSingletonKafkaProducer()).thenReturn(kafkaProducer);
    Mockito.when(controller.getSingletonPluginConfig()).thenReturn(pluginConfig);


  }

  @Test
  public void clusterSummaryPublisherTest() {
    String testClusterName = "TestRcaCluster";
    String testSummaryName = "HotClusterSummary";
    String testClusterSummary = "{\"test key\": \"test value\"}";
    JsonObject testJson = new JsonObject();
    testJson.addProperty(testSummaryName, testClusterSummary);
    HotClusterSummary testSummary = Mockito.mock(HotClusterSummary.class);
    Mockito.when(testSummary.toJson()).thenReturn(testJson);

    Map<String, HotClusterSummary> summaryMap = new HashMap<>();
    summaryMap.put(testClusterName, testSummary);

    clusterSummaryKafkaPublisher = new ClusterSummaryKafkaPublisher();
    ClusterSummary clusterSummary = Mockito.mock(ClusterSummary.class);
    Mockito.when(clusterSummary.summaryMapIsEmpty()).thenReturn(false);
    Mockito.when(clusterSummary.getSummaryMap()).thenReturn(summaryMap);

    Mockito.when(pluginConfig.getKafkaDecisionListenerConfig(Mockito.any())).thenReturn(testClusterSummaryTopic);
    clusterSummaryKafkaPublisher.setKafkaProducerController(controller);
    kafkaConsumer = generateTestConsumer();
    kafkaConsumer.subscribe(Collections.singletonList(testClusterSummaryTopic));
    clusterSummaryKafkaPublisher.summaryPublished(clusterSummary);

    ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
    kafkaConsumer.close();
    System.out.println("records count1: " + records.count());
    Assert.assertEquals(1, records.count());
    Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
    ConsumerRecord<String, String> record = recordIterator.next();
    Assert.assertEquals("{\"TestRcaCluster\":{\"HotClusterSummary\":\"{\\\"test key\\\": \\\"test value\\\"}\"}}", record.value());
  }

  @Test
  public void decisionPublisherTest() {
    decisionKafkaPublisher = new DecisionKafkaPublisher();
    String testSummary = "{\"test summary\"}";
    String actionName = "testAction";
    Mockito.when(pluginConfig.getKafkaDecisionListenerConfig(Mockito.any())).thenReturn(testActionSummaryTopic);
    Mockito.when(action.summary()).thenReturn(testSummary);
    Mockito.when(action.name()).thenReturn(actionName);
    kafkaConsumer = generateTestConsumer();
    kafkaConsumer.subscribe(Collections.singletonList(testActionSummaryTopic));
    decisionKafkaPublisher.setKafkaProducerController(controller);
    decisionKafkaPublisher.actionPublished(action);
    ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
    kafkaConsumer.close();
    System.out.println("records count2: " + records.count());
    Assert.assertEquals(1, records.count());
    Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
    ConsumerRecord<String, String> record = recordIterator.next();
    Assert.assertEquals("{\"test summary\"}", record.value());
  }

  public KafkaConsumer<String, String> generateTestConsumer() {
    //setup sample kafka consumer
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, testBootstrapServer);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer");
    return new KafkaConsumer<>(consumerProps);
  }
}
