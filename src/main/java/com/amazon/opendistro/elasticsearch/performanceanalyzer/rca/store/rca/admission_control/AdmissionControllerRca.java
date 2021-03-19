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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AdmissionControllerRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {

    private static final Logger LOG = LogManager.getLogger(AdmissionControllerRca.class);

    // Global JVM Memory Pressure Metric
    private static final String GLOBAL_JVMMP = "Global_JVMMP";

    // Request Size Metric
    private static final String REQUEST_SIZE = "Request_Size";

    private final Metric admissionControlCurrentValue;
    private final Metric admissionControlThresholdValue;
    private final Metric admissionControlRejectionCount;

    public <M extends Metric> AdmissionControllerRca(final int rcaPeriod,
                                                     final M admissionControlCurrentValue,
                                                     final M admissionControlThresholdValue,
                                                     final M admissionControlRejectionCount) {
        super(rcaPeriod);

        this.admissionControlCurrentValue = admissionControlCurrentValue;
        this.admissionControlThresholdValue = admissionControlThresholdValue;
        this.admissionControlRejectionCount = admissionControlRejectionCount;
    }

    private <M extends Metric> double getMaxMetricValue(String controllerName, M metric) {
        double metricValue = 0;
        for (MetricFlowUnit metricFU : metric.getFlowUnits()) {
            if (metricFU.isEmpty()) {
                continue;
            }
            if (metricFU.getData().isEmpty()) {
                continue;
            }
            try {
                metricValue = SQLParsingUtil.readDataFromSqlResult(
                        metricFU.getData(),
                        AllMetrics.AdmissionControlDimension.CONTROLLER_NAME.getField(),
                        controllerName,
                        MetricsDB.MAX);
                if (Double.isNaN(metricValue)) {
                    metricValue = 0;
                    LOG.warn("[AdmissionControl] Failed to parse metric from {}", metric.name());
                } else {
                    LOG.debug("[AdmissionControl] Metric value {} is {}", metric.name(), metricValue);
                }
            } catch (Exception ex) {
                LOG.warn("[AdmissionControl] Failed to parse metric from {}. [{}]", metric.name(), ex.toString());
            }
        }
        return metricValue;
    }

    private AdmissionControlMetrics getMetricModel(String controllerName) {
        AdmissionControlMetrics metricValue = new AdmissionControlMetrics();
        metricValue.controllerName = controllerName;
        metricValue.rejectionCount = (long)getMaxMetricValue(controllerName, admissionControlRejectionCount);
        metricValue.thresholdValue = (long)getMaxMetricValue(controllerName, admissionControlThresholdValue);
        metricValue.currentValue = (long)getMaxMetricValue(controllerName, admissionControlCurrentValue);
        return metricValue;
    }

    @Override
    public ResourceFlowUnit<HotNodeSummary> operate() {
        long currentTimeMillis = System.currentTimeMillis();

        AdmissionControlMetrics globalJVMMP = getMetricModel(GLOBAL_JVMMP);
        AdmissionControlMetrics requestSize = getMetricModel(REQUEST_SIZE);

        LOG.info("[AdmissionControl] Time:{} Controller:{} CurrentValue:{} ThresholdValue:{} RejectionCount:{}",
                currentTimeMillis, GLOBAL_JVMMP, globalJVMMP.currentValue, globalJVMMP.thresholdValue, globalJVMMP.rejectionCount);

        LOG.info("[AdmissionControl] Time:{} Controller:{} CurrentValue:{} ThresholdValue:{} RejectionCount:{}",
                currentTimeMillis, REQUEST_SIZE, requestSize.currentValue, requestSize.thresholdValue, requestSize.rejectionCount);

        // TODO: Add the RCA logic here
        return new ResourceFlowUnit<>(currentTimeMillis);
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        throw new IllegalArgumentException(name() + ": not expected to be called over the wire");
    }

    public static class AdmissionControlMetrics {

        private String controllerName;
        private long currentValue;
        private long thresholdValue;
        private long rejectionCount;

        public AdmissionControlMetrics() {
            super();
        }

        public AdmissionControlMetrics(String controllerName, long currentValue, long thresholdValue, long rejectionCount) {
            super();
            this.controllerName = controllerName;
            this.currentValue = currentValue;
            this.thresholdValue = thresholdValue;
            this.rejectionCount = rejectionCount;
        }

        public String getControllerName() {
            return controllerName;
        }

        public long getCurrentValue() {
            return currentValue;
        }

        public long getThresholdValue() {
            return thresholdValue;
        }

        public long getRejectionCount() {
            return rejectionCount;
        }
    }

}
