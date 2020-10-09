/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;

public class DecisionMakerConsts {
    public static final String HEAP_TUNABLE_NAME = "heap-usage";

    public static final ImmutableMap<UsageBucket, Double> HEAP_USAGE_MAP =
            ImmutableMap.<UsageBucket, Double>builder()
                    .put(UsageBucket.UNDER_UTILIZED, 30.0)
                    .put(UsageBucket.HEALTHY_WITH_BUFFER, 60.0)
                    .put(UsageBucket.HEALTHY, 90.0)
                    .build();

    public static final String CACHE_MAX_WEIGHT = "maximumWeight";

    public static final JsonParser JSON_PARSER = new JsonParser();
}
