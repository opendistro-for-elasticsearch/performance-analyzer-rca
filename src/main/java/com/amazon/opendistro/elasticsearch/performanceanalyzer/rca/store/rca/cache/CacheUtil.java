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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

public class CacheUtil {
    private static final Logger LOG = LogManager.getLogger(CacheUtil.class);

    public static Double getTotalSizeInKB(final Metric cacheSizeGroupByOperation) {
        double totalSizeInKB = 0;

        if (cacheSizeGroupByOperation.getFlowUnits().size() > 0) {
            // we expect the Metric to have single flow unit since it is consumed locally
            MetricFlowUnit flowUnit = cacheSizeGroupByOperation.getFlowUnits().get(0);
            if (flowUnit.isEmpty() || flowUnit.getData() == null) {
                return totalSizeInKB;
            }

            // since the flow unit data is aggregated by index, summing the size across indices
            if (flowUnit.getData().size() > 0) {
                Result<Record> records = flowUnit.getData();
                double size = records.stream().mapToDouble(
                        record -> record.getValue(MetricsDB.SUM, Double.class)).sum();
                totalSizeInKB += getSizeInKB(size);
            }
        }
        return totalSizeInKB;
    }

    public static Double getSizeInKB(double sizeinBytes) {
        double sizeInKB = 0;
        if (Double.isNaN(sizeinBytes)) {
            LOG.error("getSizeInKB called with NaN value");
        } else {
            sizeInKB = sizeinBytes / 1024.0;
        }
        return sizeInKB;
    }

    public static double getCacheMaxSize(AppContext appContext, NodeKey esNode, Resource cacheResource) {
        try {
            return appContext.getNodeConfigCache().get(esNode, cacheResource);
        } catch (IllegalArgumentException e) {
            return 0;
        }
    }

    public static Boolean isSizeThresholdExceeded(final Metric cacheSizeGroupByOperation,
                                                  double cacheMaxSizeinBytes,
                                                  double threshold_percentage) {
        double cacheSizeInKB = getTotalSizeInKB(cacheSizeGroupByOperation);
        double cacheMaxSizeInKB = getSizeInKB(cacheMaxSizeinBytes);
        return cacheSizeInKB != 0 && cacheMaxSizeInKB != 0 && (cacheSizeInKB > cacheMaxSizeInKB * threshold_percentage);
    }
}
