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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit.ResourceFlowUnitFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.SQLiteQueryUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.CreateTableConstraintStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStepN;
import org.jooq.JSONFormat;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

class SQLitePersistor extends PersistorBase {
  private static final String DB_URL = "jdbc:sqlite:";
  private DSLContext create;
  private Map<String, List<Field<?>>> jooqTableColumns;
  private static final Logger LOG = LogManager.getLogger(SQLitePersistor.class);
  private static final String LAST_INSERT_ROWID = "last_insert_rowid()";
  private static final String PRIMARY_KEY_AUTOINCREMENT_POSTFIX = " INTEGER PRIMARY KEY AUTOINCREMENT";

  private static int id_test = 1;

  SQLitePersistor(String dir, String filename, String storageFileRetentionCount,
                  TimeUnit rotationTime, long rotationPeriod) throws SQLException, IOException {
    super(dir, filename, DB_URL, storageFileRetentionCount, rotationTime, rotationPeriod);
    create = DSL.using(conn, SQLDialect.SQLITE);
    jooqTableColumns = new HashMap<>();
  }

  // This updates the DSL context based on a new SQLite connection
  // It is needed during SQLite file rotation
  @Override
  synchronized void createNewDSLContext() {
    create = DSL.using(super.conn, SQLDialect.SQLITE);
    jooqTableColumns = new HashMap<>();
  }

  @Override
  synchronized void createTable(String tableName, List<Field<?>> columns) {
    CreateTableConstraintStep constraintStep = create.createTable(tableName)
        //sqlite does not support identity. use plain sql string instead.
        .column(DSL.field(getPrimaryKeyColumnName(tableName) + PRIMARY_KEY_AUTOINCREMENT_POSTFIX))
        .columns(columns);

    LOG.debug("table created: {}", constraintStep.toString());
    constraintStep.execute();
    jooqTableColumns.put(tableName, columns);
  }

  /**
   * create table with foreign key
   */
  @Override
  synchronized void createTable(String tableName, List<Field<?>> columns, String referenceTableName,
      String referenceTablePrimaryKeyFieldName) throws SQLException {
    Field foreignKeyField = DSL.field(referenceTablePrimaryKeyFieldName, Integer.class);
    columns.add(foreignKeyField);
    Table referenceTable = DSL.table(referenceTableName);
    CreateTableConstraintStep constraintStep = create.createTable(tableName)
        .column(DSL.field(getPrimaryKeyColumnName(tableName) + PRIMARY_KEY_AUTOINCREMENT_POSTFIX))
        .columns(columns)
        .constraints(DSL.constraint(foreignKeyField.getName() + "_FK").foreignKey(foreignKeyField)
            .references(referenceTable, DSL.field(referenceTablePrimaryKeyFieldName)));

    LOG.debug("table with fk created: {}", constraintStep.toString());
    try {
      constraintStep.execute();
      jooqTableColumns.put(tableName, columns);
    } catch (Exception e) {
      LOG.error("Failed to create table {}", tableName);
      throw new SQLException();
    }
  }

  @Override
  synchronized int insertRow(String tableName, List<Object> row) throws SQLException {
    int lastPrimaryKey = -1;
    String sqlQuery = "SELECT " + LAST_INSERT_ROWID;
    InsertValuesStepN insertValuesStepN = create.insertInto(DSL.table(tableName))
        .columns(jooqTableColumns.get(tableName))
        .values(row);
    LOG.debug("sql insert: {}", insertValuesStepN.toString());
    try {
      insertValuesStepN.execute();
      lastPrimaryKey = create.fetch(sqlQuery).get(0).get(LAST_INSERT_ROWID, Integer.class);
    } catch (Exception e) {
      LOG.error("Failed to insert into the table {}", tableName);
      throw new SQLException();
    }
    LOG.debug("most recently inserted primary key = {}", lastPrimaryKey);
    return lastPrimaryKey;
  }

  // This reads all SQLite tables in the latest SQLite file and converts the read data to JSON.
  @Override
  synchronized String readTables() {
    JsonParser jsonParser = new JsonParser();
    JsonObject tablesObject = new JsonObject();
    super.tableNames.forEach(
        table -> {
            String tableStr = readTable(table);
            try {
              JsonElement tableElement = jsonParser.parse(tableStr);
              tablesObject.add(table, tableElement);
            }
            catch (JsonSyntaxException se) {
              LOG.error("RCA: Json parsing fails when reading from table {}", table);
            }
        }
    );
    return tablesObject.toString();
  }

  //read table content and convert it into JSON format
  private synchronized String readTable(String tableName) {
    String tableStr;
    try {
      Result<Record> result;
      if (tableName.equals(ResourceFlowUnit.RCA_TABLE_NAME)) {
        result = create.select()
            .from(tableName)
            .orderBy(ResourceFlowUnitFieldValue.RCA_NAME_FILELD.getField())
            .fetch();
      }
      else {
        result = create.select().from(tableName).fetch();
      }
      tableStr = result.formatJSON(new JSONFormat().header(false));
    } catch (DataAccessException e) {
      LOG.error("Fail to read table {}", tableName);
      tableStr = "[]";
    }
    return tableStr;
  }

  // TODO: we only query the most recent RCA entry in this API. might need to extend this
  // to support range query based on timestamp.
  @Override
  public synchronized RcaResponse readRca(String rca) {
    RcaResponse response = null;
    Field<Integer> primaryKeyField = DSL.field(
        SQLiteQueryUtils.getPrimaryKeyColumnName(ResourceFlowUnit.RCA_TABLE_NAME), Integer.class);
    SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils.buildRcaQuery(create, rca);
    try {
      List<Record> recordList = rcaQuery.fetch();
      if (recordList.size() > 0) {
        Record mostRecentRecord = recordList.get(0);
        response = RcaResponse.buildResponse(mostRecentRecord);
        if (response.getState().equals(State.UNHEALTHY.toString())) {
          readSummary(response, mostRecentRecord.get(primaryKeyField));
        }

        if (rca.equals(ClusterTemperatureRca.TABLE_NAME)) {
          Field<Integer> foreignKeyField = DSL.field(
                  SQLiteQueryUtils.getPrimaryKeyColumnName(ResourceFlowUnit.RCA_TABLE_NAME), Integer.class);
          SelectJoinStep<Record> query = SQLiteQueryUtils
                  .buildSummaryQuery(create, ClusterTemperatureSummary.TABLE_NAME,
                          mostRecentRecord.get(primaryKeyField),
                          foreignKeyField);
          try {
            Result<Record> temperaureSummary = query.fetch();
            GenericSummary summary =
                    ClusterTemperatureSummary.buildSummaryFromDatabase(temperaureSummary, create);
            response.addNestedSummaryList(summary);
          } catch (DataAccessException dex) {
            dex.printStackTrace();
          }
        }
      }
    } catch (DataAccessException de) {
      // it is totally fine if we fail to read some certain tables.
      LOG.warn("Fail to read RCA : {}, query = {},  exceptions : {}", rca, rcaQuery.toString(), de.getStackTrace());
    }
    return response;
  }

  private void readSummary(GenericSummary upperLevelSummary, int upperLevelPrimaryKey) {
    String upperLevelTable = upperLevelSummary.getTableName();
    List<Class<? extends GenericSummary>> clazzList = SQLiteQueryUtils.getNestedTableMap().getOrDefault(upperLevelSummary.getClass(), null);

    // stop the recursion here if the table does not have any nested summary.
    if (clazzList == null) {
      return;
    }
    for (Class<? extends GenericSummary> clazz : clazzList) {
      Field<Integer> foreignKeyField = DSL.field(
          SQLiteQueryUtils.getPrimaryKeyColumnName(upperLevelTable), Integer.class);
      SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils
          .buildSummaryQuery(create, clazz.getSimpleName(), upperLevelPrimaryKey, foreignKeyField);
      try {
        Result<Record> recordList = rcaQuery.fetch();
        for (Record record : recordList) {
          Method method = clazz.getMethod("buildSummary", Record.class);
          GenericSummary summary = (GenericSummary) method.invoke(null, record);
          if (summary != null) {
            Field<Integer> primaryKeyField = DSL.field(
                SQLiteQueryUtils.getPrimaryKeyColumnName(summary.getTableName()), Integer.class);
            readSummary(summary, record.get(primaryKeyField));
            upperLevelSummary.addNestedSummaryList(summary);
          }
        }
      }
      catch (DataAccessException de) {
        // it is totally fine if we fail to read some certain tables as some types of summaries might be missing
        LOG.warn("Fail to read Summary table : {}, query = {},  exceptions : {}",
            clazz.getSimpleName(), rcaQuery.toString(), de.getStackTrace());
      } catch (Exception e) {
        // we might got a reflection issue if running into this. Check the NestedTableMap and make sure
        // the summary class has been added.
        LOG.error("Fail to use reflection to build GenericSummary, trace = {}", e.getStackTrace());
      }
    }
  }
}
