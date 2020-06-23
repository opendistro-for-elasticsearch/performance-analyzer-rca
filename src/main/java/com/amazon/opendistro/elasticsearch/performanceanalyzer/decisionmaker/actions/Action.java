package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

public interface Action {

    /**
     * Time to wait since last recommendation, before suggesting this action again
     */
    int coolOffPeriodInSeconds();

    /**
     * Returns an ImpactVector indicating the impact of this action on key resources.
     */
    ImpactVector impact();

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
