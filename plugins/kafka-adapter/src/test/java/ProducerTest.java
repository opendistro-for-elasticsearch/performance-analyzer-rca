import com.harold.configuration.ProducerConfiguration;
import com.harold.tool.Helper;
import com.harold.starter.ProducerStarter;
import com.harold.tool.Target;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProducerTest {

    private Target target1;
    private Target target2;
    private Target target3;
    private ProducerConfiguration config1;
    private ProducerConfiguration config2;
    private ProducerConfiguration config3;

    private String bootstrapServer1 = "localhost:9092";
    private String bootstrapServer2; //support Customer configured server
    private String topic1 = "rca";
    private String topic2 = "wrongTopic";

    @Before
    public void producerTestSetup() {
        target1 = new Target("localhost:9600/_opendistro/_performanceanalyzer/rca");
        target2 = new Target("localhost:9600/_opendistro/_performanceanalyzer/rca","ClusterTemperatureRca");
        target3 = new Target("wrongurl", "ClusterTemperatureRca");
        config1 = new ProducerConfiguration(bootstrapServer1, topic1,6000);
        config2 = new ProducerConfiguration(bootstrapServer1, topic2,0);
        config3 = new ProducerConfiguration(bootstrapServer1, topic1,50000);

        KafkaProducer<String, JsonNode> producer = config1.CreateProducer();
        assertNotNull(producer);
        assertNotNull(Helper.makeRequest(target1));
        assertNotNull(Helper.makeRequest(target2));
    }

    @Test
    public void testSetup(){
        assertEquals(6000, config1.getInterval());
        assertEquals("wrongTopic", config2.getTopic());
        assertEquals(5000, config3.getInterval());
        config3.setBootstrap_server("localhost:9000");
        assertEquals("localhost:9000", config3.getBootstrap_server());
    }

    @Test
    public void kafkaProducerTest(){
        ProducerStarter.writeToKafkaQueue(target1, config1, 1);
        ProducerStarter.writeToKafkaQueue(target2, config1, 1);
    }
}