package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import java.util.List;
import java.util.Map;

public interface Action {

    /**
     * Constant to indicate that an action impacts all nodes in the cluster
     */
    final String ALL_NODES = "all_nodes";

    /**
     * Time to wait since last recommendation, before suggesting this action again
     */
    int coolOffPeriodInSeconds();

    /**
     * Returns a list of Elasticsearch NodeIds impacted by this action.
     * {@link ALL_NODES} is used for cluster wide actions impacting all nodes.
     */
    List<String> impactedNodes();

    /**
     * Returns a map of Elasticsearch nodeId to ImpactVector of this action on that node
     */
    Map<String, ImpactVector> impact();

    /**
     * Returns Action Name
     */
    String getName();

    /**
     * Called when the action is invoked.
     *
     * Specific implementation may include executing the action, or invoking downstream APIs
     */
    void execute();
}
