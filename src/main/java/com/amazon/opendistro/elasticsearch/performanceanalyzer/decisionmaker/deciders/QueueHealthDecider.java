package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;

public class QueueHealthDecider extends Decider {

    private QueueRejectionClusterRca queueRejectionRca;

    public QueueHealthDecider(long evalIntervalSeconds, int decisionFrequency, QueueRejectionClusterRca queueRejectionClusterRca) {
        super(evalIntervalSeconds, decisionFrequency);
        this.queueRejectionRca = queueRejectionClusterRca;
    }

    @Override
    public Decision operate() {
        return null;
    }
}
