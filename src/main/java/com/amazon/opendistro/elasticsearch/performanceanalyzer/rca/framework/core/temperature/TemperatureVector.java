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

import com.google.gson.JsonObject;
import java.util.Comparator;
import javax.annotation.Nullable;

public class TemperatureVector {
    public enum Dimension {
        CPU_Utilization(com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization.NAME),
        Heap_AllocRate(com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_AllocRate.NAME),
        IO_READ_SYSCALL_RATE(com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IO_ReadSyscallRate.NAME),
        IO_WriteSyscallRate(com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IO_WriteSyscallRate.NAME),
        Shard_Size(com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Shard_Size.NAME);

        public final String NAME;

        Dimension(String name) {
            this.NAME = name;
        }
    }

    public static class NormalizedValue {
        public static final int MIN = 0;
        public static final int MAX = 10;

        private final short POINTS;

        public NormalizedValue(short heatValue) {
            if (heatValue < MIN || heatValue > MAX) {
                String err = String.format("Only values between %d and %d allowed. Given: %d", MIN, MAX,
                        heatValue);
                throw new IllegalArgumentException(err);
            }

            this.POINTS = heatValue;
        }

        /**
         * Units of a resource consumed is a number between 0 and 10 where 0 being cold(not using any
         * resource) and 10 being hot(almost all the units of the resource were consumed by it).
         * Temperature is calculated by determining what parts of 10 is consumed by the resource.
         */
        public static NormalizedValue calculate(double consumedByCandidate, double totalConsumption) {
            return new NormalizedValue((short) (consumedByCandidate * 10 / totalConsumption));
        }

        public NormalizedValue diff(NormalizedValue b) {
            return new NormalizedValue((short) (POINTS - b.POINTS));
        }

        public boolean isGreaterThan(NormalizedValue b) {
            return POINTS > b.POINTS;
        }

        public Comparator<NormalizedValue> comparator() {
            return Comparator.comparingInt(o -> o.POINTS);
        }

        public short getPOINTS() {
            return POINTS;
        }

        @Override
        public String toString() {
            return "" + POINTS;
        }
    }

    /**
     * This array contains a normalized value per dimension.
     */
    private NormalizedValue[] normalizedValues;

    public TemperatureVector() {
        normalizedValues = new NormalizedValue[Dimension.values().length];
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        String delimiter = "";
        for (Dimension dim : Dimension.values()) {
            String key = dim.NAME;
            NormalizedValue value = normalizedValues[dim.ordinal()];
            stringBuilder.append(delimiter).append(key).append(":").append(value);
            delimiter = ",";
        }

        return "TemperatureVector{"
                + stringBuilder.toString()
                + '}';
    }

    public JsonObject toJson() {
        JsonObject jsonObject = new JsonObject();
        for (Dimension dim : Dimension.values()) {
            jsonObject.addProperty("dimension", dim.NAME);

            NormalizedValue val = normalizedValues[dim.ordinal()];
            jsonObject.addProperty("value",
                    val != null ? val.getPOINTS() : null);
        }
        return jsonObject;
    }

    /**
     * This can be used to get temperature along a dimension.
     *
     * @param dimension one of the dime
     * @return The normalized temperature value along that dimension.
     */
    @Nullable
    public NormalizedValue getTemperatureFor(Dimension dimension) {
        return normalizedValues[dimension.ordinal()];
    }

    public void updateTemperatureForDimension(Dimension dimension,
                                              NormalizedValue normalizedValue) {
        normalizedValues[dimension.ordinal()] = normalizedValue;
    }
}
