import com.opendestro.kafkaAdapter.configuration.ConsumerConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConsumerTest {

    private ConsumerConfiguration config1;
    static String WEBHOOK_URL = "https://hooks.slack.com/services/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";

    @Before
    public void consumerTestSetup() {
        config1 = new ConsumerConfiguration("localhost:9092", "rca", 5000);
        KafkaConsumer<String, JsonNode> consumer1 = config1.createConsumer();
        assertNotNull(consumer1);
    }

    @Test
    public void testSetup(){
        assertEquals("rca", config1.getTopic());
        assertEquals(5000,config1.getInterval());
        config1.setBootstrap_server("localhost:9093");
        assertEquals("localhost:9093", config1.getBootstrap_server());
        config1.setBootstrap_server("localhost:9092");
        config1.setInterval(99999);
        assertEquals(5000, config1.getInterval());
        config1.setTopic("wrongTopic");
        config1.setInterval(3000);
        assertEquals("wrongTopic", config1.getTopic());
        assertEquals(3000,config1.getInterval());

    }

}