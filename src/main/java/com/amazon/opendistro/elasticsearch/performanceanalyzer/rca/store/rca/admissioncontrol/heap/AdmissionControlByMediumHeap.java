/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.heap;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State.HEALTHY;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State.UNHEALTHY;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.HEAP_MAX_SIZE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaVerticesMetrics.ADMISSION_CONTROL_RCA_TRIGGERED;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.model.HeapMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range.Range;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range.RangeConfiguration;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** AdmissionControl RCA for heap > 4gb and <= 32gb */
public class AdmissionControlByMediumHeap implements AdmissionControlByHeap {

    private static final Logger LOG = LogManager.getLogger(AdmissionControlByMediumHeap.class);
    private double previousHeapPercent = 0;

    private InstanceDetails instanceDetails;
    private RangeConfiguration requestSizeHeapRange;

    @Override
    public void init(InstanceDetails instanceDetails, RangeConfiguration rangeConfiguration) {
        this.instanceDetails = instanceDetails;
        this.requestSizeHeapRange = rangeConfiguration;
    }

    @Override
    public ResourceFlowUnit<HotNodeSummary> generateFlowUnits(HeapMetric heapMetric) {

        long currentTimeMillis = System.currentTimeMillis();
        double currentHeapPercent = heapMetric.getHeapPercent();

        HotNodeSummary nodeSummary =
                new HotNodeSummary(
                        instanceDetails.getInstanceId(), instanceDetails.getInstanceIp());

        // If we observe heap percent range change then we tune request-size controller threshold
        // by marking resource as unhealthy and setting desired value as configured
        if (requestSizeHeapRange.hasRangeChanged(previousHeapPercent, currentHeapPercent)) {

            double currentThreshold = getThreshold(requestSizeHeapRange, currentHeapPercent);
            if (currentThreshold == 0) {
                // AdmissionControl rejects all requests if threshold is set to 0, thus ignoring
                return new ResourceFlowUnit<>(
                        currentTimeMillis,
                        new ResourceContext(HEALTHY),
                        nodeSummary,
                        !instanceDetails.getIsMaster());
            }

            LOG.debug(
                    "[AdmissionControl] PreviousHeapPercent={} CurrentHeapPercent={} CurrentDesiredThreshold={}",
                    previousHeapPercent,
                    currentHeapPercent,
                    currentThreshold);

            double previousThreshold = getThreshold(requestSizeHeapRange, previousHeapPercent);
            previousHeapPercent = currentHeapPercent;

            HotResourceSummary resourceSummary =
                    new HotResourceSummary(HEAP_MAX_SIZE, currentThreshold, previousThreshold, 0);
            nodeSummary.appendNestedSummary(resourceSummary);

            RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
                    ADMISSION_CONTROL_RCA_TRIGGERED, instanceDetails.getInstanceId().toString(), 1);

            return new ResourceFlowUnit<>(
                    currentTimeMillis,
                    new ResourceContext(UNHEALTHY),
                    nodeSummary,
                    !instanceDetails.getIsMaster());
        }

        return new ResourceFlowUnit<>(
                currentTimeMillis,
                new ResourceContext(HEALTHY),
                nodeSummary,
                !instanceDetails.getIsMaster());
    }

    private double getThreshold(RangeConfiguration heapRange, double heapPercent) {
        Range range = heapRange.getRange(heapPercent);
        return Objects.isNull(range) ? 0 : range.getThreshold();
    }
}
