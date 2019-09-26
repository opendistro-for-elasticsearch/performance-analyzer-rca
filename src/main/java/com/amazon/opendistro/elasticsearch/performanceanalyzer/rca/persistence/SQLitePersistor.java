package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

class SQLitePersistor extends PersistorBase {
  private static final String DB_URL = "jdbc:sqlite:";
  private static final Logger LOG = LogManager.getLogger(SQLitePersistor.class);
  private final DSLContext create;
  private Map<String, List<Field<String>>> jooqTableColumns;

  SQLitePersistor(String dir, String filename) throws SQLException {
    super(dir, filename, DB_URL);
    create = DSL.using(conn, SQLDialect.SQLITE);
    jooqTableColumns = new HashMap<>();
  }

  @Override
  void createTable(String tableName, List<String> columns) {
    List<Field<String>> fields =
        columns.stream()
            .map(s -> DSL.field(DSL.name(s), String.class))
            .collect(Collectors.toList());
    create.createTable(tableName).columns(fields).execute();
    jooqTableColumns.put(tableName, fields);
  }

  @Override
  void insertRow(String tableName, List<String> row) {
    LOG.info("RCA: Table: {}, Row:\n{}", tableName, row);
    create
        .insertInto(DSL.table(tableName))
        .columns(jooqTableColumns.get(tableName))
        .values(row)
        .execute();
  }
}
