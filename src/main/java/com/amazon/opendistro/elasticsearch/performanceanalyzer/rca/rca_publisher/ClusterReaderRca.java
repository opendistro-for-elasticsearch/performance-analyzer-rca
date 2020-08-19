/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterReaderRca<T extends GenericSummary> extends NonLeafNode<EmptyFlowUnit> {
    private static final Logger LOG = LogManager.getLogger(ClusterReaderRca.class);
    public static final String NAME = "ClusterReaderRca";
    private final List<Rca<ResourceFlowUnit<T>>> clusterRcas;
    private ClusterSummary<T> clusterSummary;
    private List<ClusterSummaryListener<T>> clusterSummaryListeners;

    public ClusterReaderRca(final int evalIntervalSeconds, List<Rca<ResourceFlowUnit<T>>> clusterRcas) {
        super(0, evalIntervalSeconds);
        this.clusterRcas = clusterRcas;
        clusterSummary = new ClusterSummary<>(evalIntervalSeconds, new HashMap<>());
        clusterSummaryListeners = new ArrayList<>();
    }

    public String name() {
        return NAME;
    }

    public String getAllClusterRcaName() {
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

    public List<ClusterSummaryListener<T>> getClusterSummaryListeners(){
        return clusterSummaryListeners;
    }

    public void addClusterSummaryListener(ClusterSummaryListener<T> listener){
        clusterSummaryListeners.add(listener);
    }

    @Override
    public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
        LOG.debug("ClusterReaderRca: reading Rca data from: {}", getAllClusterRcaName());
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
                RcaGraphMetrics.GRAPH_NODE_OPERATE_CALL, name(), duration);
    }

    @Override
    public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
        assert true;
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
        for (Rca<ResourceFlowUnit<T>> clusterRca : clusterRcas) {
            List<ResourceFlowUnit<T>> clusterFlowUnits = clusterRca.getFlowUnits();
            if (clusterFlowUnits.isEmpty()) {
                continue;
            }
            if (clusterFlowUnits.get(0).hasResourceSummary()) {
                clusterSummary.addSummary(clusterRca.name(), clusterRca.getFlowUnits().get(0).getSummary());
            }
        }
        for(ClusterSummaryListener<T> listener: clusterSummaryListeners){
            listener.summaryPublished(clusterSummary);
        }
        return new EmptyFlowUnit(System.currentTimeMillis());
    }
}
