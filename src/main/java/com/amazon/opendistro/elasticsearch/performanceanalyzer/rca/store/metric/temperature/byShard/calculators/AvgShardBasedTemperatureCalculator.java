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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.calculators;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectSeekStep1;
import org.jooq.impl.DSL;

/**
 * This builds over the query from {@code SumOverOperationsForIndexShardGroup}.
 * It calculates the average over all index,shard groups.
 */
public class AvgShardBasedTemperatureCalculator extends ShardBasedTemperatureCalculator {
    public AvgShardBasedTemperatureCalculator(TemperatureDimension metricType) {
        super(metricType);
    }

    public static final String ALIAS = "sum_max";
    public static final String SHARD_AVG = "shard_avg";

    protected Field<?> getAggrDimension() {
        return super.getAggrDimension().as(ALIAS);
    }

    // This uses the return from the getSumOfUtilByIndexShardGroup as inner query and gets an
    // average over all index-shard groups.
    @Override
    protected Result<Record> createDslAndFetch(final DSLContext context,
                                               final String tableName,
                                               final Field<?> aggDimension,
                                               final List<Field<?>> groupByFieldsList,
                                               final List<Field<?>> selectFieldsList) {
        SelectSeekStep1<Record, ?> sumByIndexShardGroupsClause =
                getSumOfUtilByIndexShardGroup(context, tableName, aggDimension, groupByFieldsList, selectFieldsList);

        selectFieldsList.clear();

        Field<?> avgOverShards = DSL.avg(DSL.field(DSL.name(ALIAS), Double.class)).as("shard_avg");

        selectFieldsList.add(avgOverShards);
        Result<?> r = context.select(avgOverShards).from(sumByIndexShardGroupsClause).fetch();

        return (Result<Record>) r;
    }
}
