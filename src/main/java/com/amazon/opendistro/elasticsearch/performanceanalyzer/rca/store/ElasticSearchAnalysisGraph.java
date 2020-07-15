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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_LOCUS;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.AnalysisGraphComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.QueueRejectionGraphComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.ResourceHeatMapGraphComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.ResourcesGraphComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.ShardResourceUsageGraphComponent;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Monitor;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ElasticSearchAnalysisGraph extends AnalysisGraph {
    private final Monitor localStateMonitor = new Monitor();
    private final AnalysisGraphDecisionMakerManager decisionMakerManager = new AnalysisGraphDecisionMakerManager(this);

    @GuardedBy("localStateMonitor") private final Map<String, Metric> metricsInRegistry = new ConcurrentHashMap<>();
    @GuardedBy("localStateMonitor") private final Map<String, Rca> rcaInRegistry = new ConcurrentHashMap<>();

    @Override
    public void construct() {
        final ImmutableList<AnalysisGraphComponent> components =
            new ImmutableList.Builder<AnalysisGraphComponent>()
                    .add(new ResourcesGraphComponent(this))
                    .add(new ShardResourceUsageGraphComponent(this))
                    .add(new ResourceHeatMapGraphComponent(this))
                    .add(new QueueRejectionGraphComponent(this))
                    .build();

        // Registers all leaf node metrics
        components.forEach((component) -> component.getMetrics().forEach(this::getOrRegisterMetric));

        // Registers all rca
        components.forEach(AnalysisGraphComponent::registerRca);

        // Registers deciders, collators and publishers
        decisionMakerManager.registerDecisionMaker();
    }

    Metric getOrRegisterMetric(final Metric metric) {
        final String metricName = metric.name();

        localStateMonitor.enter();
        try {
            final Optional<Metric> existing = verifyMetricExisting(metricName);
            if (existing.isPresent()) {
                return metricsInRegistry.get(metricName);
            }

            metric.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
            addLeaf(metric);

            metricsInRegistry.put(metricName, metric);
            return metric;
        } finally {
            localStateMonitor.leave();
        }
    }

    public Rca getOrRegisterRca(final Rca rca) {
        final String rcaName = rca.name();

        localStateMonitor.enter();
        try {
            final Optional<Rca> existing = verifyRcaExisting(rcaName);
            if (existing.isPresent()) {
                return rcaInRegistry.get(rcaName);
            }

            rcaInRegistry.put(rcaName, rca);
            return rca;
        } finally {
            localStateMonitor.leave();
        }
    }

    public Optional<Metric> getMetric(final String metricName) {
        return verifyMetricExisting(metricName);
    }

    public Optional<Rca> getRca(final String rcaName) {
        return verifyRcaExisting(rcaName);
    }

    private Optional<Metric> verifyMetricExisting(final String metricName) {
        localStateMonitor.enter();
        try {
            final Metric metric = metricsInRegistry.get(metricName);
            if (metric == null) {
                return Optional.empty();
            }
            return Optional.of(metric);
        } finally {
            localStateMonitor.leave();
        }
    }

    private Optional<Rca> verifyRcaExisting(final String metricName) {
        localStateMonitor.enter();
        try {
            final Rca rca = rcaInRegistry.get(metricName);
            if (rca == null) {
                return Optional.empty();
            }
            return Optional.of(rca);
        } finally {
            localStateMonitor.leave();
        }
    }
}
