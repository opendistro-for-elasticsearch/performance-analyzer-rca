package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.EmptyFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterReaderRca<T extends GenericSummary> extends NonLeafNode<EmptyFlowUnit> {
    private static final Logger LOG = LogManager.getLogger(ClusterReaderRca.class);
    public static final String NAME = "ClusterReaderRca";
    private final List<Rca<ResourceFlowUnit<T>>> clusterRcas;
    private final Map<String, T> summaryMap;

    public ClusterReaderRca(final int evalIntervalSeconds, List<Rca<ResourceFlowUnit<T>>> clusterRcas) {
        super(0, evalIntervalSeconds);
        this.clusterRcas = clusterRcas;
        summaryMap = new HashMap<>();
    }

    public String name() {
        return NAME;
    }

    @Override
    public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
        LOG.debug("ClusterReaderRca: reading Rca data from: {}", getClusterRcaName());
        long startTime = System.currentTimeMillis();
        try {
            this.operate();
        } catch (Exception e) {
            LOG.error("ClusterReaderRca: Exception in operate", e);
            PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
                    ExceptionsAndErrors.EXCEPTION_IN_OPERATE, name(), 1);
        }
        long duration = System.currentTimeMillis() - startTime;
        PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
                RcaGraphMetrics.GRAPH_NODE_OPERATE_CALL, this.name(), duration);
    }

    @Override
    public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
        assert true;
    }

    public String getClusterRcaName() {
        StringBuilder list = new StringBuilder();
        clusterRcas.forEach((cluster) -> {
            list.append(cluster.name()).append(", ");
        });
        return list.toString();
    }

    public void addClusterRca(Rca<ResourceFlowUnit<T>> rca) {
        clusterRcas.add(rca);
    }

    public List<Rca<ResourceFlowUnit<T>>> getClusterRcas() {
        return this.clusterRcas;
    }

    public boolean hasSummary() {
        return !summaryMap.isEmpty();
    }

    public Map<String, T> getSummaryMap(){
        return this.summaryMap;
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
                + "be required.");
    }

    @Override
    public void handleNodeMuted() {
        assert true;
    }

    @Override
    public EmptyFlowUnit operate() {
        for (int i = 0; i < clusterRcas.size(); i++) {
            Rca<ResourceFlowUnit<T>> clusterRca = clusterRcas.get(i);
            List<ResourceFlowUnit<T>> clusterFlowUnits = clusterRca.getFlowUnits();
            //TODO: get flow unit of each cluster rca, need to check cluster summary
            if (clusterFlowUnits.isEmpty()) {
                continue;
            }
            if (clusterFlowUnits.get(0).hasResourceSummary()) {
                summaryMap.put(clusterRca.name(), clusterRca.getFlowUnits().get(0).getSummary());
            }
        }
        return new EmptyFlowUnit(Instant.now().toEpochMilli());
    }
}
