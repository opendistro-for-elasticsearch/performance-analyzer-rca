package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

import java.util.List;
import java.util.Map;

public interface Action {

    /**
     * Returns true if the configured action is actionable, false otherwise.
     *
     * Examples of non-actionable actions are resource configurations where
     * limits have been reached.
     */
    boolean isActionable();

    /**
     * Time to wait since last recommendation, before suggesting this action again
     */
    int coolOffPeriodInSeconds();

    /**
     * Called when the action is invoked.
     *
     * Specific implementation may include executing the action, or invoking downstream APIs
     */
    void execute();

    /**
     * Returns a list of Elasticsearch nodes impacted by this action.
     */
    List<NodeKey> impactedNodes();

    /**
     * Returns a map of Elasticsearch nodes to ImpactVector of this action on that node
     */
    Map<NodeKey, ImpactVector> impact();

}
