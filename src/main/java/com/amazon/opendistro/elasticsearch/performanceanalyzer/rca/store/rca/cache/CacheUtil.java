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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CacheUtil {
    private static final Logger LOG = LogManager.getLogger(CacheUtil.class);

    public static Double getTotalSizeInKB(final Metric cacheSizeGroupByOperation) {
        double sizeTotalInKB = 0;

        if (cacheSizeGroupByOperation.getFlowUnits().size() > 0) {
            // we expect the Metric to have single flow unit since it is consumed locally
            MetricFlowUnit flowUnit = cacheSizeGroupByOperation.getFlowUnits().get(0);
            if (flowUnit.isEmpty() || flowUnit.getData() == null) {
                return sizeTotalInKB;
            }

            // since the flow unit data is aggregated, we should have a single value
            if (flowUnit.getData().size() > 0) {
                double size = flowUnit.getData().get(0).getValue(MetricsDB.SUM, Double.class);
                if (Double.isNaN(size)) {
                    LOG.error("Failed to parse metric in FlowUnit from {}", cacheSizeGroupByOperation.getClass().getName());
                } else {
                    sizeTotalInKB += size / 1024.0;
                }
            }
        }
        return sizeTotalInKB;
    }

    public static Boolean isSizeThresholdExceeded(final Metric cacheSizeGroupByOperation,
                                                  final Metric cacheMaxSizeGroupByOperation,
                                                  double threshold_percentage) {
        double cacheSize = getTotalSizeInKB(cacheSizeGroupByOperation);
        double cacheMaxSize = getTotalSizeInKB(cacheMaxSizeGroupByOperation);
        return cacheSize != 0 && cacheMaxSize != 0 && (cacheSize > cacheMaxSize * threshold_percentage);
    }
}

