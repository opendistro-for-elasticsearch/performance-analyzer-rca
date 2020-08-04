package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class DecisionToKafkaPlugin extends Plugin implements ActionListener {

    public final String NAME = "DecisionToKafkaPlugin";
    private static final Logger LOG = LogManager.getLogger(PublisherEventsLogger.class);

    @Override
    public void actionPublished(Action action) {
        LOG.info("Action: [{}] published by decision maker publisher.", action.name());
        //TODO: Write into kakfa queue.
        HashMap<String, String> messages = (HashMap<String, String>) getMessagesForNodes(action);


    }



    public Map<String, String> getMessagesForNodes(Action action){ //To generate MessageMap including decisions for each node
        List<NodeKey> NodeKeyList = action.impactedNodes(); //List of node involved in the action
        Map<NodeKey, ImpactVector> map = action.impact();
        Map<String, StringBuilder> messageMap = new HashMap<>(); // Map to store the decision within each NodeKey
        for(NodeKey node : NodeKeyList){
            String nodeID = node.getNodeId().toString();
            ImpactVector impactVector = map.get(node); // Get the impact vector of the node
            if(impactVector == null) continue;
            Map<ImpactVector.Dimension, ImpactVector.Impact> dimensionMap = impactVector.getImpact();
            for(Map.Entry<ImpactVector.Dimension, ImpactVector.Impact> entry: dimensionMap.entrySet()){
                if(!entry.getValue().equals(ImpactVector.Impact.NO_IMPACT)){
                    StringBuilder sb = messageMap.getOrDefault(nodeID, new StringBuilder());
                    sb.append(String.format("Dimension: %s, is made for decision: %s, ",entry.getKey(), entry.getValue()));
                    messageMap.put(nodeID, sb);
                }
            }
        }

        return messageMap.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().toString())
        );
    }


    public void publishToKafkaQueue(){
        assert true;
    }

    @Override
    public String name() {
        return NAME;
    }
}
