package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HeapUtil {
    private static final Logger LOG = LogManager.getLogger(HeapUtil.class);

    public static double getHeapUsedPercentage(final Long heapMaxInBytes, final Long heapUsedInBytes) {
        double heapUsedPercent = 0;

        if (heapMaxInBytes != null && heapMaxInBytes > 0 && heapUsedInBytes != null && heapUsedInBytes > 0) {
            heapUsedPercent = ((double) heapUsedInBytes / heapMaxInBytes) * 100;
        }

        if (!Double.isNaN(heapUsedPercent)) {
            return heapUsedPercent;
        } else {
            throw new IllegalArgumentException("invalid value: {} in heapUsedPercent" + Float.NaN);
        }
    }
}
