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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_CLUSTER_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_NODE_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_RESOURCE_SUMMARY_TABLE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.QueryUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaResponseUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.jooq.Table;
import org.jooq.impl.DSL;

class SQLitePersistor extends PersistorBase {
  private static final String DB_URL = "jdbc:sqlite:";
  private DSLContext create;
  private Map<String, List<Field<?>>> jooqTableColumns;
  private static final Logger LOG = LogManager.getLogger(SQLitePersistor.class);
  private static final String LAST_INSERT_ROWID = "last_insert_rowid()";
  private static final String PRIMARY_KEY_AUTOINCREMENT_POSTFIX = " INTEGER PRIMARY KEY AUTOINCREMENT";

  private static int id_test = 1;

  SQLitePersistor(String dir, String filename) throws SQLException {
    super(dir, filename, DB_URL);
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
      String referenceTablePrimaryKeyFieldName) {
    Field foreignKeyField = DSL.field(referenceTablePrimaryKeyFieldName, Integer.class);
    columns.add(foreignKeyField);
    Table referenceTable = DSL.table(referenceTableName);
    CreateTableConstraintStep constraintStep = create.createTable(tableName)
        .column(DSL.field(getPrimaryKeyColumnName(tableName) + PRIMARY_KEY_AUTOINCREMENT_POSTFIX))
        .columns(columns)
        .constraints(DSL.constraint(foreignKeyField.getName() + "_FK").foreignKey(foreignKeyField)
            .references(referenceTable, DSL.field(referenceTablePrimaryKeyFieldName)));

    LOG.debug("table with fk created: {}", constraintStep.toString());
    constraintStep.execute();
    jooqTableColumns.put(tableName, columns);
  }

  @Override
  synchronized int insertRow(String tableName, List<Object> row) {
    InsertValuesStepN insertValuesStepN = create.insertInto(DSL.table(tableName))
        .columns(jooqTableColumns.get(tableName))
        .values(row);
    LOG.debug("sql insert: {}", insertValuesStepN.toString());
    insertValuesStepN.execute();
    int lastPrimaryKey = -1;
    String sqlQuery = "SELECT " + LAST_INSERT_ROWID;
    try {
      lastPrimaryKey = create.fetch(sqlQuery).get(0).get(LAST_INSERT_ROWID, Integer.class);
    } catch (Exception e) {
      LOG.error("Failed to query the table {} , query : {}", tableName, sqlQuery);
    }
    LOG.debug("most recently inserted primary key = {}", lastPrimaryKey);
    return lastPrimaryKey;
  }

  // This reads all SQLite tables in the latest SQLite file and converts the read data to JSON.
  @Override
  synchronized String readTables() {
    StringBuilder jsonResult = new StringBuilder();
    final JSONFormat jsonFormat = new JSONFormat().header(false);
    jsonResult.append("{");
    // Currently returning a json string response generated by jooq library formatJSON method
    // Need to change this because currently maps are being stored as strings in JSON instead of
    // objects
    String tableJson =
        super.tableNames.stream()
            .map(
                table -> {
                  StringBuilder jsonForEachTable = new StringBuilder();
                  jsonForEachTable.append("\"" + table + "\":");
                  Result<Record> result = create.select().from(table).fetch();
                  jsonForEachTable.append(result.formatJSON(jsonFormat));
                  return jsonForEachTable.toString();
                })
            .collect(Collectors.joining(","));
    jsonResult.append(tableJson);
    jsonResult.append("}");
    LOG.debug("JSON Result - " + jsonResult);
    return jsonResult.toString();
  }

  @Override
  synchronized RcaResponse readRcaTable(String rca) {
    Set<String> tableNames = super.tableNames;
    if (!tableNames.contains(rca)) {
      return null;
    }
    List<Record> rcaResponseRecordList = QueryUtils.getRcaRecordList(create, rca, getSummaryTableMap(rca), tableNames);
    return RcaResponseUtil.getRcaResponse(rca, rcaResponseRecordList, tableNames);
  }

  private Map<String,String> getSummaryTableMap(String rca) {
    Map<String,String> map = new LinkedHashMap<>();
    map.put(HOT_CLUSTER_SUMMARY_TABLE, getPrimaryKeyColumnName(rca));
    map.put(HOT_NODE_SUMMARY_TABLE, getPrimaryKeyColumnName(HOT_CLUSTER_SUMMARY_TABLE));
    map.put(HOT_RESOURCE_SUMMARY_TABLE, getPrimaryKeyColumnName(HOT_NODE_SUMMARY_TABLE));
    return map;
  }
}
