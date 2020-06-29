package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;

import java.util.ArrayList;
import java.util.List;

// This is a sample decider implementation to finalize decision maker interfaces.
// TODO: 1. Get threadpool type from HotNodeSummary or HotResourceSummary
// TODO: 2. Read action priorities from a configurable yml
// TODO: 3. Read current queue capacity from NodeConfigurationRca (PR #252)
// TODO: 4. Add unit tests

public class QueueHealthDecider extends Decider {

    public static String NAME = "queue_health";

    private QueueRejectionClusterRca queueRejectionRca;
    List<String> actionsByUserPriority = new ArrayList<>();
    private int counter = 0;

    public QueueHealthDecider(long evalIntervalSeconds, int decisionFrequency, QueueRejectionClusterRca queueRejectionClusterRca) {
        // TODO: Also consume NodeConfigurationRca
        super(evalIntervalSeconds, decisionFrequency);
        this.queueRejectionRca = queueRejectionClusterRca;
        configureActionPriority();
    }

    @Override
    public Decision operate() {
        Decision decision = new Decision(System.currentTimeMillis(), NAME);
        counter += 1;
        if (counter < decisionFrequency) {
            return decision;
        }

        counter = 0;
        if (queueRejectionRca.getFlowUnits().isEmpty()) {
            return decision;
        }

        HotClusterSummary clusterSummary = getLatestFlowUnit(queueRejectionRca.getFlowUnits()).getSummary();
        for (HotNodeSummary nodeSummary: clusterSummary.getHotNodeSummaryList()) {
            NodeKey esNode = new NodeKey(nodeSummary.getNodeID(), nodeSummary.getHostAddress());
            // TODO: Only create action for the specific unhealthy threadpool via HotResourceSummary. For now, assuming all queues unhealthy
            decision.addAction(computeBestAction(esNode, ThreadPoolType.WRITE));
            decision.addAction(computeBestAction(esNode, ThreadPoolType.SEARCH));
        }

        return decision;
    }

    private void configureActionPriority() {
        // TODO: Input from user configured yml
        this.actionsByUserPriority.add(QueueCapacity.NAME);
    }

    /**
     * Evaluate the most relevant action for a node
     *
     * Action relevance decided based on user configured priorities for now, this can be modified to
     * consume better signals going forward.
     */
    private Action computeBestAction(NodeKey esNode, ThreadPoolType threadPoolType) {
        Action action = null;
        for (String actionName: actionsByUserPriority) {
            action = getAction(actionName, esNode, threadPoolType, getNodeQueueCapacity(esNode, threadPoolType), true);
            if (action != null) {
                break;
            }
        }
        return action;
    }

    private Action getAction(String actionName, NodeKey esNode, ThreadPoolType threadPoolType, int currCapacity, boolean increase) {
        switch(actionName) {
            case QueueCapacity.NAME: return configureQueueCapacity(esNode, threadPoolType, currCapacity, increase);
            default: return null;
        }
    }

    private QueueCapacity configureQueueCapacity(NodeKey esNode, ThreadPoolType threadPoolType, int currentCapacity, boolean increase) {
        QueueCapacity action = new QueueCapacity(esNode, threadPoolType, currentCapacity, increase);
        if (action.isActionable()) {
            return action;
        }
        return null;
    }

    private int getNodeQueueCapacity(NodeKey esNode, ThreadPoolType threadPoolType) {
        // TODO: use NodeConfigurationRca to return capacity, for now returning defaults
        if (threadPoolType == ThreadPoolType.SEARCH) {
            return 1000;
        }
        return 100;
    }

    private ResourceFlowUnit<HotClusterSummary> getLatestFlowUnit(List<ResourceFlowUnit<HotClusterSummary>> flowUnits) {
        long ts = 0;
        ResourceFlowUnit<HotClusterSummary> latest = null;
        for (ResourceFlowUnit<HotClusterSummary> flowUnit: flowUnits) {
            if (flowUnit.getTimeStamp() > ts) {
                latest = flowUnit;
            }
        }
        return latest;
    }
}
