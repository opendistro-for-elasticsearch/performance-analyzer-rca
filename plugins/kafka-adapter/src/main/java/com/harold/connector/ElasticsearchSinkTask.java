//package connector;
//
//import com.sun.tools.internal.ws.wsdl.document.Output;
//import org.apache.kafka.connect.sink.SinkTask;
//
//import java.util.Map;
//
//public class ElasticsearchSinkTask extends SinkTask {
//    String filename;
//    Output stream;
//    String topic;
//
//    @Override
//    public void start(Map<String, String> props) {
//        filename = props.get(ElasticsearchSinkConnector.FILE_CONFIG);
//        stream = openOrThrowError(filename);
//        topic = props.get(ElasticsearchSinkConnector.TOPIC_CONFIG);
//    }
//
//    private Output openOrThrowError(String filename) {
//
//    }
//    }
//
//    @Override
//    public synchronized void stop() {
//        stream.close();
//    }
//}