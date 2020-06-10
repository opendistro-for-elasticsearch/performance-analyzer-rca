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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.SummaryBuilder;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.NodeLevelDimensionalSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.SQLiteQueryUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.NodeTemperatureRca;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
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
          } catch (JsonSyntaxException se) {
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
      } else {
        result = create.select().from(tableName).fetch();
      }
      tableStr = result.formatJSON(new JSONFormat().header(false));
    } catch (DataAccessException e) {
      LOG.error("Fail to read table {}", tableName);
      tableStr = "[]";
    }
    return tableStr;
  }

  /**
   * FullTemperatureSummary is not one single Rca, instead it is a conglomeration of
   * temperature across all dimensions. Therefore, it iterates over all the dimensional tables
   * to arrive at that result.
   *
   * @return Returns a JsonObject with full temperature profile.
   */
  private JsonElement constructFullTemperatureProfile() {
    JsonObject rcaResponseJson = null;
    JsonArray nodeDimensionalSummary = null;
    String summaryName = NodeLevelDimensionalSummary.SUMMARY_TABLE_NAME;

    // We use the JsonObject returned as part of the first dimension as the template and then
    // for each subsequent dimension, we extend the json Array we have from the first dimension.
    for (String dimension : SQLiteQueryUtils.temperatureProfileDimensionRCASet) {
      if (rcaResponseJson == null) {
        rcaResponseJson = readTemperatureProfileRca(dimension).getAsJsonObject();
        JsonElement elem = rcaResponseJson.get(summaryName);
        if (elem == null) {
          rcaResponseJson = null;
          continue;
        }
        nodeDimensionalSummary = rcaResponseJson.get(summaryName).getAsJsonArray();
        nodeDimensionalSummary.get(0).getAsJsonObject().addProperty(
            ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME,
            rcaResponseJson.get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME).getAsString());

      } else {
        JsonObject resp = readTemperatureProfileRca(dimension).getAsJsonObject();
        if (resp != null && resp.getAsJsonObject().get(summaryName) != null) {
          JsonObject obj =
              resp.getAsJsonObject().get(summaryName).getAsJsonArray().get(0).getAsJsonObject();
          obj.addProperty(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME,
              resp.get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME).getAsString());
          nodeDimensionalSummary.add(obj);
        }
      }
    }

    if (rcaResponseJson != null) {
      // This is the name of the first dimension ans hence confusing. This element is redundant
      // anyways as the top level object is the name of the RCA queried for.
      rcaResponseJson.remove(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.RCA_COL_NAME);

      // State of a temperature profile RCA has no meaning. SO we remove this.
      rcaResponseJson.remove(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME);

      // Timestamp field exists for each dimension. A top level timestamp is not required.
      rcaResponseJson.remove(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME);
    }
    return rcaResponseJson;
  }

  private void readSummary(GenericSummary upperLevelSummary, int upperLevelPrimaryKey) {
    String upperLevelTable = upperLevelSummary.getTableName();

    // stop the recursion here if the summary does not have any nested summary table.
    for (String nestedTableName : upperLevelSummary.getNestedSummaryTables()) {
      Field<Integer> foreignKeyField = DSL.field(
          SQLiteQueryUtils.getPrimaryKeyColumnName(upperLevelTable), Integer.class);
      SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils
          .buildSummaryQuery(create, nestedTableName, upperLevelPrimaryKey, foreignKeyField);
      try {
        Result<Record> recordList = rcaQuery.fetch();
        for (Record record : recordList) {
          GenericSummary summary = upperLevelSummary.buildNestedSummary(nestedTableName, record);
          if (summary != null) {
            Field<Integer> primaryKeyField = DSL.field(
                SQLiteQueryUtils.getPrimaryKeyColumnName(summary.getTableName()), Integer.class);
            readSummary(summary, record.get(primaryKeyField));
          }
        }
      } catch (DataAccessException de) {
        // it is totally fine if we fail to read some certain tables as some types of summaries might be missing
        LOG.warn("Fail to read Summary table : {}, query = {},  exceptions : {}",
            nestedTableName, rcaQuery.toString(), de.getStackTrace());
      }
    }
  }

  private JsonElement getTemperatureRca(String rca) {
    JsonElement temperatureRcaJson;
    switch (rca) {
      case SQLiteQueryUtils.ALL_TEMPERATURE_DIMENSIONS:
        temperatureRcaJson = constructFullTemperatureProfile();
        break;
      default:
        temperatureRcaJson = readTemperatureProfileRca(rca);
    }
    return temperatureRcaJson;
  }

  private JsonElement getNonTemperatureRcas(String rca) {
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
      }
    } catch (DataAccessException de) {
      // it is totally fine if we fail to read some certain tables.
      LOG.warn("Fail to read RCA : {}, query = {},  exceptions : {}", rca, rcaQuery.toString(), de.getStackTrace());
    }
    JsonElement ret = null;
    if (response != null) {
      ret = response.toJson();
    }
    return ret;
  }

  // TODO: we only query the most recent RCA entry in this API. might need to extend this
  // to support range query based on timestamp.
  @Override
  public synchronized JsonElement readRca(String rca) {
    JsonElement json;
    if (SQLiteQueryUtils.isTemperatureProfileRca(rca)) {
      json = getTemperatureRca(rca);
    } else {
      json = getNonTemperatureRcas(rca);
    }
    return json;
  }

  private JsonElement readTemperatureProfileRca(String rca) {
    RcaResponse response = null;
    Field<Integer> primaryKeyField = DSL.field(
        SQLiteQueryUtils.getPrimaryKeyColumnName(ResourceFlowUnit.RCA_TABLE_NAME), Integer.class);
    SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils.buildRcaQuery(create, rca);
    try {
      List<Record> recordList = rcaQuery.fetch();
      if (recordList == null || recordList.isEmpty()) {
        return new JsonObject();
      }
      Record mostRecentRecord = recordList.get(0);
      response = RcaResponse.buildResponse(mostRecentRecord);

      // ClusterTemperatureRca can only be retrieved from the elected master. If the request is
      // made from a data node, it returns a 400 saying it can only be queried from the elected
      // master.
      if (rca.equals(ClusterTemperatureRca.TABLE_NAME)) {
        Field<Integer> foreignKeyField = DSL.field(
            SQLiteQueryUtils.getPrimaryKeyColumnName(ResourceFlowUnit.RCA_TABLE_NAME),
            Integer.class);
        SelectJoinStep<Record> query = SQLiteQueryUtils
            .buildSummaryQuery(create, ClusterTemperatureSummary.TABLE_NAME,
                mostRecentRecord.get(primaryKeyField),
                foreignKeyField);
        Result<Record> temperatureSummary = query.fetch();
        GenericSummary summary =
            ClusterTemperatureSummary.buildSummaryFromDatabase(temperatureSummary, create);
        response.addNestedSummaryList(summary);
      } else if (rca.equalsIgnoreCase(NodeTemperatureRca.TABLE_NAME)) {
        SelectJoinStep<Record> query = SQLiteQueryUtils.buildSummaryQuery(
            create,
            CompactNodeSummary.TABLE_NAME,
            mostRecentRecord.get(primaryKeyField),
            primaryKeyField);
        Result<Record> nodeTemperatureCompactSummary = query.fetch();
        GenericSummary summary =
            CompactNodeSummary.buildSummaryFromDatabase(
                nodeTemperatureCompactSummary, create);
        response.addNestedSummaryList(summary);
      } else {
        // This gives you the full temperature profile for this node.
        SelectJoinStep<Record> query = SQLiteQueryUtils.buildSummaryQuery(
            create,
            NodeLevelDimensionalSummary.SUMMARY_TABLE_NAME,
            mostRecentRecord.get(primaryKeyField),
            primaryKeyField);
        Result<Record> result = query.fetch();
        GenericSummary nodeLevelDimSummary =
            NodeLevelDimensionalSummary.buildFromDb(result.get(0), create);
        response.addNestedSummaryList(nodeLevelDimSummary);
      }
    } catch (DataAccessException dex) {
      LOG.error("Failed to read temperature profile RCA for {}", rca, dex);
      if (dex.getMessage().contains("no such table")) {
        JsonObject json = new JsonObject();
        json.addProperty("error", "RCAs are not created yet.");
        return json;
      }
    }
    return response.toJson();
  }
}
