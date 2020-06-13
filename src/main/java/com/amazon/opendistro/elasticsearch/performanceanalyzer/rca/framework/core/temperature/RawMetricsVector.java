/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import javax.annotation.Nullable;

/**
 * This class contains the vector details which contains the raw metrics values.
 * We have added this vector to compare against the calculated normalized values of the
 * metrics so as to catch any anomalies whatsoever.
 */

public class RawMetricsVector {
    public static final String DIMENSION_KEY = "metrics_dimension";
    public static final String VALUE_KEY = "metrics_value";

    public static double[] metricsValues;

    public RawMetricsVector() {
        metricsValues = new double[TemperatureDimension.values().length];
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    public JsonArray toJson() {
        JsonArray array = new JsonArray();
        for (TemperatureDimension dim : TemperatureDimension.values()) {
            double val = metricsValues[dim.ordinal()];
            if (val != 0) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty(DIMENSION_KEY, dim.NAME);
                jsonObject.addProperty(VALUE_KEY, val);
                array.add(jsonObject);
            }
        }
        return array;
    }

    /**
     * This can be used to get raw metrics along a dimension.
     *
     * @param dimension one of the dimensions
     * @return The raw metrics value along that dimension.
     */
    @Nullable
    public double getMetricsFor(TemperatureDimension dimension) {
        return metricsValues[dimension.ordinal()];
    }

    public void updateRawMetricsForDimension(TemperatureDimension dimension,
                                             double metricsValue) {
        metricsValues[dimension.ordinal()] = metricsValue;
    }
}