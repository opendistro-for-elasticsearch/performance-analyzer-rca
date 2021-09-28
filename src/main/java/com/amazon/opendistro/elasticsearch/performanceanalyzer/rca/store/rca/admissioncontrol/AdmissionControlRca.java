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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.HEAP;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State.HEALTHY;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil.readDataFromSqlResult;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.AdmissionControlRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.heap.AdmissionControlByHeap;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.heap.AdmissionControlByHeapFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.model.HeapMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range.Range;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range.RangeConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.range.RequestSizeHeapRangeConfiguration;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;

public class AdmissionControlRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {
    private static final Logger LOG = LogManager.getLogger(AdmissionControlRca.class);
    private static final double BYTES_TO_GIGABYTES = Math.pow(1024, 3);

    // Global JVM Memory Pressure Metric
    public static final String GLOBAL_JVMMP = "Global_JVMMP";
    // Request Size Metric
    public static final String REQUEST_SIZE = "Request_Size";

    private final Metric heapUsedValue;
    private final Metric heapMaxValue;

    private final RangeConfiguration requestSizeHeapRange;
    private final int rcaPeriod;
    private int counter;

    public <M extends Metric> AdmissionControlRca(
            final int rcaPeriodInSeconds, final M heapUsedValue, final M heapMaxValue) {
        super(rcaPeriodInSeconds);
        this.counter = 0;
        this.rcaPeriod = rcaPeriodInSeconds;
        this.heapUsedValue = heapUsedValue;
        this.heapMaxValue = heapMaxValue;
        this.requestSizeHeapRange = new RequestSizeHeapRangeConfiguration();
    }

    private <M extends Metric> double getMetric(M metric, Field<String> field, String fieldName) {
        double response = 0;
        for (MetricFlowUnit flowUnit : metric.getFlowUnits()) {
            if (!flowUnit.isEmpty()) {
                double metricResponse =
                        readDataFromSqlResult(flowUnit.getData(), field, fieldName, MetricsDB.MAX);
                if (!Double.isNaN(metricResponse) && metricResponse > 0) {
                    response = metricResponse;
                }
            }
        }
        return response;
    }

    private HeapMetric getHeapMetric() {
        double usedHeapInGb =
                getMetric(heapUsedValue, MEM_TYPE.getField(), HEAP.toString()) / BYTES_TO_GIGABYTES;
        double maxHeapInGb =
                getMetric(heapMaxValue, MEM_TYPE.getField(), HEAP.toString()) / BYTES_TO_GIGABYTES;
        return new HeapMetric(usedHeapInGb, maxHeapInGb);
    }

    /**
     * read threshold values from rca.conf
     *
     * @param conf RcaConf object
     */
    @Override
    public void readRcaConf(RcaConf conf) {
        AdmissionControlRcaConfig rcaConfig = conf.getAdmissionControlRcaConfig();
        AdmissionControlRcaConfig.ControllerConfig requestSizeConfig =
                rcaConfig.getRequestSizeControllerConfig();
        List<Range> requestSizeHeapRangeConfiguration =
                requestSizeConfig.getHeapRangeConfiguration();
        if (requestSizeHeapRangeConfiguration != null
                && requestSizeHeapRangeConfiguration.size() > 0) {
            requestSizeHeapRange.setRangeConfiguration(requestSizeHeapRangeConfiguration);
        }
    }

    @Override
    public ResourceFlowUnit<HotNodeSummary> operate() {
        long currentTimeMillis = System.currentTimeMillis();

        counter++;
        if (counter < rcaPeriod) {
            return new ResourceFlowUnit<>(currentTimeMillis);
        }
        counter = 0;

        InstanceDetails instanceDetails = getInstanceDetails();
        HotNodeSummary nodeSummary =
                new HotNodeSummary(
                        instanceDetails.getInstanceId(), instanceDetails.getInstanceIp());

        HeapMetric heapMetric = getHeapMetric();
        if (!heapMetric.hasValues()) {
            return new ResourceFlowUnit<>(
                    currentTimeMillis,
                    new ResourceContext(HEALTHY),
                    nodeSummary,
                    !instanceDetails.getIsMaster());
        }

        AdmissionControlByHeap admissionControlByHeap =
                AdmissionControlByHeapFactory.getByMaxHeap(heapMetric.getMaxHeap());
        admissionControlByHeap.init(instanceDetails, requestSizeHeapRange);
        return admissionControlByHeap.generateFlowUnits(heapMetric);
    }

    public RangeConfiguration getRequestSizeHeapRange() {
        return this.requestSizeHeapRange;
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        final List<FlowUnitMessage> flowUnitMessages =
                args.getWireHopper().readFromWire(args.getNode());
        final List<ResourceFlowUnit<HotNodeSummary>> flowUnitList = new ArrayList<>();
        LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
        for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
            flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
        }
        setFlowUnits(flowUnitList);
    }
}
