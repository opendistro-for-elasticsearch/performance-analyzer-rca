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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admission_control;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp.RCA_VERTICES_METRICS_AGGREGATOR;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.HEAP;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State.HEALTHY;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State.UNHEALTHY;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil.readDataFromSqlResult;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.HEAP_MAX_SIZE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaVerticesMetrics.ADMISSION_CONTROL_RCA_TRIGGERED;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.AdmissionControlRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range.Range;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range.RangeConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range.RequestSizeHeapRangeConfiguration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;

public class AdmissionControlRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {
    private static final Logger LOG = LogManager.getLogger(AdmissionControlRca.class);
    private static final double BYTES_TO_MEGABYTES = Math.pow(1024, 2);

    // Global JVM Memory Pressure Metric
    public static final String GLOBAL_JVMMP = "Global_JVMMP";
    // Request Size Metric
    public static final String REQUEST_SIZE = "Request_Size";

    private final Metric heapUsedValue;
    private final Metric heapMaxValue;

    private final RangeConfiguration requestSizeHeapRange = new RequestSizeHeapRangeConfiguration();
    private final int rcaPeriod;
    private int counter;

    private double previousHeapPercent = 0.0;

    public <M extends Metric> AdmissionControlRca(
            final int rcaPeriodInSeconds, final M heapUsedValue, final M heapMaxValue) {
        super(rcaPeriodInSeconds);
        this.counter = 0;
        this.rcaPeriod = rcaPeriodInSeconds;
        this.heapUsedValue = heapUsedValue;
        this.heapMaxValue = heapMaxValue;
    }

    private <M extends Metric> double getMetric(
            M metric, Field<String> field, String fieldName, String dataField) {
        AtomicReference<Double> metricValue = new AtomicReference<>((double) 0);
        metric.getFlowUnits().stream()
                .filter(flowUnit -> !flowUnit.isEmpty() && !flowUnit.getData().isEmpty())
                .mapToDouble(flowUnit -> readDataFromSqlResult(flowUnit.getData(), field, fieldName, dataField))
                .forEach(metricResponse -> {
                            if (Double.isNaN(metricResponse)) {
                                LOG.debug("[AdmissionControl] Failed to parse metric from {}", metric.name());
                            } else {
                                metricValue.set(metricResponse);
                            }
                });
        return metricValue.get();
    }

    private HeapMetrics getHeapMetric() {
        HeapMetrics heapMetrics = new HeapMetrics();
        heapMetrics.usedHeap =
                getMetric(heapUsedValue, MEM_TYPE.getField(), HEAP.toString(), MetricsDB.MAX) / BYTES_TO_MEGABYTES;
        heapMetrics.maxHeap =
                getMetric(heapMaxValue, MEM_TYPE.getField(), HEAP.toString(), MetricsDB.MAX) / BYTES_TO_MEGABYTES;
        return heapMetrics;
    }

    /**
     * read threshold values from rca.conf
     *
     * @param conf RcaConf object
     */
    @Override
    public void readRcaConf(RcaConf conf) {
        AdmissionControlRcaConfig rcaConfig = conf.getAdmissionControlRcaConfig();
        AdmissionControlRcaConfig.ControllerConfig requestSizeConfig = rcaConfig.getRequestSizeControllerConfig();
        List<Range> requestSizeHeapRangeConfiguration = requestSizeConfig.getHeapRangeConfiguration();
        if (requestSizeHeapRangeConfiguration != null && requestSizeHeapRangeConfiguration.size() > 0) {
            requestSizeHeapRange.setRangeConfiguration(requestSizeHeapRangeConfiguration);
        }
    }

    @Override
    public ResourceFlowUnit<HotNodeSummary> operate() {
        long currentTimeMillis = System.currentTimeMillis();

        counter++;
        if (counter < rcaPeriod) {
            return new ResourceFlowUnit<>(currentTimeMillis, new ResourceContext(HEALTHY), null);
        }
        counter = 0;

        HeapMetrics heapMetrics = getHeapMetric();
        if (heapMetrics.usedHeap == 0 || heapMetrics.maxHeap == 0) {
            return new ResourceFlowUnit<>(currentTimeMillis, new ResourceContext(HEALTHY), null);
        }
        double currentHeapPercent = (heapMetrics.usedHeap / heapMetrics.maxHeap) * 100;

        // If we observe heap percent range change then we tune request-size controller threshold
        // by marking resource as unhealthy and setting desired value as configured
        if (requestSizeHeapRange.hasRangeChanged(previousHeapPercent, currentHeapPercent)) {
            double desiredThreshold = getHeapBasedThreshold(currentHeapPercent);
            if (desiredThreshold == 0) {
                // AdmissionControl rejects all requests if threshold is set to 0, thus ignoring
                return new ResourceFlowUnit<>(currentTimeMillis, new ResourceContext(HEALTHY), null);
            }
            LOG.debug(
                    "[AdmissionControl] Observed range change. previousHeapPercent={} currentHeapPercent={} desiredThreshold={}",
                    previousHeapPercent,
                    currentHeapPercent,
                    desiredThreshold);

            previousHeapPercent = currentHeapPercent;
            InstanceDetails instanceDetails = getInstanceDetails();

            HotResourceSummary resourceSummary =
                    new HotResourceSummary(HEAP_MAX_SIZE, desiredThreshold, currentHeapPercent, 0);
            HotNodeSummary nodeSummary =
                    new HotNodeSummary(instanceDetails.getInstanceId(), instanceDetails.getInstanceIp());
            nodeSummary.appendNestedSummary(resourceSummary);

            RCA_VERTICES_METRICS_AGGREGATOR.updateStat(
                    ADMISSION_CONTROL_RCA_TRIGGERED, instanceDetails.getInstanceId().toString(), 1);

            return new ResourceFlowUnit<>(currentTimeMillis, new ResourceContext(UNHEALTHY), nodeSummary);
        }

        return new ResourceFlowUnit<>(currentTimeMillis, new ResourceContext(HEALTHY), null);
    }

    private double getHeapBasedThreshold(double currentHeapPercent) {
        Range range = requestSizeHeapRange.getRange(currentHeapPercent);
        return Objects.isNull(range) ? 0 : range.getThreshold();
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        throw new IllegalArgumentException(name() + ": not expected to be called over the wire");
    }

    private static class HeapMetrics {
        private double usedHeap;
        private double maxHeap;
    }
}
