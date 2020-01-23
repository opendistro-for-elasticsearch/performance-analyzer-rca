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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;

/**
 * A utility class to query cluster, node and resource level summary for a rca
 */
public class QueryUtils {

  /**
   * This function fetches cluster, node and resource summary records for a rca
   *
   * @param ctx DSLContext
   * @param rca The rca that will be queried
   * @param summaryTableToForeignKeyMap Map of summaryTables which will be queried to get rca's summary
   * @param validTables The tables currently present in database
   * @return list of rca records representing node and resource level information for input rca
   */
  public static List<Record> getRcaRecordList(DSLContext ctx,
                                              String rca,
                                              Map<String, String> summaryTableToForeignKeyMap,
                                              Set<String> validTables) {
    SelectJoinStep<Record> rcaQuery = ctx.select().from(rca);
    long timestamp = getMaxTimestampLessThanOrEqualTo(ctx, rca, System.currentTimeMillis());
    return performSummaryJoin(rcaQuery, validTables, summaryTableToForeignKeyMap)
            .where(DSL.field(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME).equal(timestamp))
            .fetch();
  }

  private static SelectJoinStep<Record> performSummaryJoin(SelectJoinStep<Record> joinStep,
                                                           Set<String> validTables,
                                                           Map<String, String> summaryTableToForeignKeyMap) {
    for (Map.Entry<String, String> entry : summaryTableToForeignKeyMap.entrySet()) {
      if (validTables.contains(entry.getKey())) {
        joinStep = doJoin(joinStep, entry.getKey(), entry.getValue());
      } else {
        return joinStep;
      }
    }
    return joinStep;
  }

  private static SelectJoinStep<Record> doJoin(SelectJoinStep<Record> joinStep,
                                               String tableName,
                                               String fieldName) {
    return joinStep.innerJoin(tableName)
            .using(DSL.field(fieldName));
  }

  private static long getMaxTimestampLessThanOrEqualTo(DSLContext ctx,
                                                       String tableName,
                                                       long timestamp) {
    String maxTimestamp = (String) ctx.select(DSL.max(DSL.field(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME)))
            .from(tableName)
            .where(DSL.field(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME).lessOrEqual(timestamp))
            .fetch()
            .get(0)
            .value1();
    return Long.parseLong(maxTimestamp);
  }
}


