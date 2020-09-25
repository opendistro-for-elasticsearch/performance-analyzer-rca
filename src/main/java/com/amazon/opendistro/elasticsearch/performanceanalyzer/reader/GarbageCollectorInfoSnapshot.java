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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCInfoDimension;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public class GarbageCollectorInfoSnapshot implements Removable {

  private static final Logger LOG = LogManager.getLogger(GarbageCollectorInfoSnapshot.class);

  private final DSLContext create;
  private final Long windowStartTime;
  private final String tableName;
  private List<Field<?>> columns;


  public enum Fields {
    MEMORY_POOL(GCInfoDimension.MEMORY_POOL.toString()),
    COLLECTOR_NAME(GCInfoDimension.COLLECTOR_NAME.toString());

    private final String fieldValue;

    Fields(String fieldValue) {
      this.fieldValue = fieldValue;
    }


    @Override
    public String toString() {
      return fieldValue;
    }
  }

  public GarbageCollectorInfoSnapshot(Connection conn, Long windowStartTime) {
    this.create = DSL.using(conn, SQLDialect.SQLITE);
    this.windowStartTime = windowStartTime;
    this.tableName = "gc_info_" + windowStartTime;
    this.columns = new ArrayList<Field<?>>() {
      {
        this.add(DSL.field(DSL.name(Fields.MEMORY_POOL.toString()), String.class));
        this.add(DSL.field(DSL.name(Fields.COLLECTOR_NAME.toString()), String.class));
      }
    };

    create.createTable(tableName).columns(columns).execute();
  }

  public BatchBindStep startBatchPut() {
    List<Object> dummyValues = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      dummyValues.add("");
    }
    return create.batch(create.insertInto(DSL.table(this.tableName)).values(dummyValues));
  }

  public Result<Record> fetchAll() {
    return create.select().from(DSL.table(tableName)).fetch();
  }

  @Override
  public void remove() throws Exception {
    create.dropTable(DSL.table(tableName)).execute();
  }
}
