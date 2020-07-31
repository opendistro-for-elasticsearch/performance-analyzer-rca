//package connector;
//
//import org.apache.kafka.common.config.ConfigDef;
//import org.apache.kafka.connect.connector.Task;
//import org.apache.kafka.connect.sink.SinkConnector;
//
//import java.io.OutputStream;
//import java.util.*;
//
//public class ElasticsearchSinkConnector extends SinkConnector {
//    private static final String FILE_CONFIG = "";
//    private static final String TOPIC_CONFIG = "";
//    private String filename;
//    private String topic;
//    private OutputStream stream;
//    private Properties props;
//
//    @Override
//    public void start(Map<String, String> map) {
//        try{
//            filename = (String) props.get(FILE_CONFIG);
//            topic = (String) props.get(TOPIC_CONFIG);
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public Class<? extends Task> taskClass() {
////        return ElasticsearchSinkClass.class;
//    }
//
//    @Override
//    public List<Map<String, String>> taskConfigs(int i) {
//        ArrayList<Map<String, String>> configs = new ArrayList<>();
//        // Only one input stream makes sense.
//        Map<String, String> config = new HashMap<>();
//        if (filename != null)
//            config.put(FILE_CONFIG, filename);
//        config.put(TOPIC_CONFIG, topic);
//        configs.add(config);
//        return configs;
//    }
//
//    @Override
//    public void stop() {
//
//    }
//
//    @Override
//    public ConfigDef config() {
//        return null;
//    }
//
//    @Override
//    public String version() {
//        return null;
//    }
//}
