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

/**
 * Units of a resource consumed is a number between 0 and 10 where 0 being cold(not using any
 * resource) and 10 being hot(almost all the units of the resource were consumed by it).
 * Temperature is calculated by determining what parts of 10 is consumed by the resource.
 */
public class NormalizedConsumption {
    public static TemperatureVector.NormalizedValue calculate(double consumedByCandidate, double totalConsumption) {
        return new TemperatureVector.NormalizedValue((short) (consumedByCandidate * 10 / totalConsumption));
    }
}
