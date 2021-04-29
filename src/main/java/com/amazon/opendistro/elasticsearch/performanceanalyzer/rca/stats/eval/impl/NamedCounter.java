/*
 * Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl;


import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.NamedAggregateValue;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NamedCounter implements IStatistic<NamedAggregateValue> {

    private static final Logger LOG = LogManager.getLogger(NamedCounter.class);
    private boolean empty;
    private Map<String, NamedAggregateValue> counters;

    public NamedCounter() {
        counters = new ConcurrentHashMap<>();
        empty = true;
    }

    @Override
    public Statistics type() {
        return Statistics.NAMED_COUNTERS;
    }

    @Override
    public void calculate(String key, Number value) {
        synchronized (this) {
            NamedAggregateValue mapValue =
                    counters.getOrDefault(
                            key, new NamedAggregateValue(0L, Statistics.NAMED_COUNTERS, key));
            try {
                Number numb = mapValue.getValue();
                long number = mapValue.getValue().longValue();
                long newNumber = number + 1;
                mapValue.update(newNumber);
                counters.put(key, mapValue);
                empty = false;
            } catch (Exception ex) {
                LOG.error("Caught an exception while calculating the counter value", ex);
            }
        }
    }

    @Override
    public Collection<NamedAggregateValue> get() {
        return counters.values();
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }
}
