/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

public class CacheUtil {
    private static final Logger LOG = LogManager.getLogger(CacheUtil.class);
    private static final double CONVERT_BYTES_TO_MEGABYTES = Math.pow(1024, 3);

    public static Double getTotalSizeInMB(final Metric sizeMetric) {
        double sizeTotalInMB = 0;

        // we expect the Metric to have single flow unit since it is consumed locally
        MetricFlowUnit flowUnit = sizeMetric.getFlowUnits().get(0);
        if (flowUnit.isEmpty() || flowUnit.getData() == null) {
            return sizeTotalInMB;
        }

        for (Record record : flowUnit.getData()) {
            double size = record.getValue(MetricsDB.MAX, Double.class);
            if (Double.isNaN(size)) {
                LOG.error("Failed to parse metric in FlowUnit from {}", sizeMetric.getClass().getName());
            } else {
                sizeTotalInMB += size / CONVERT_BYTES_TO_MEGABYTES;
            }
        }
        return sizeTotalInMB;
    }
}
