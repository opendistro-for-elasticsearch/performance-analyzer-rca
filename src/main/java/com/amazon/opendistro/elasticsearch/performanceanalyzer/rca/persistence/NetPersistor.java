package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@SuppressWarnings("unchecked")
public class NetPersistor {
    private static final Logger LOG = LogManager.getLogger(NetPersistor.class);
    private static final int PER_GRAPH_NODE_FLOW_UNIT_QUEUE_CAPACITY = 200;
    ConcurrentMap<String, BlockingQueue<FlowUnitWrapper>> graphNodeToFlowUnitMap = new ConcurrentHashMap<>();

    public List<FlowUnitWrapper> read(final String graphNode) {
        if (graphNodeToFlowUnitMap.containsKey(graphNode)) {
            final BlockingQueue<FlowUnitWrapper> flowUnitQueue = graphNodeToFlowUnitMap.get(graphNode);
            final List<FlowUnitWrapper> returnList = new ArrayList<>();
            flowUnitQueue.drainTo(returnList);
            return returnList;
        }

        return new ArrayList<>();
    }

    public void write(final String graphNode, final FlowUnitMessage flowUnitMessage) {
        graphNodeToFlowUnitMap.putIfAbsent(graphNode, new ArrayBlockingQueue<>(PER_GRAPH_NODE_FLOW_UNIT_QUEUE_CAPACITY));
        final BlockingQueue<FlowUnitWrapper> flowUnitQueue = graphNodeToFlowUnitMap.get(graphNode);

        if (!flowUnitQueue.offer(FlowUnitWrapper.buildFlowUnitWrapperFromMessage(flowUnitMessage))) {
            LOG.debug("Failed to add flow unit to the buffer. Dropping the flow unit.");
        }
    }
}
