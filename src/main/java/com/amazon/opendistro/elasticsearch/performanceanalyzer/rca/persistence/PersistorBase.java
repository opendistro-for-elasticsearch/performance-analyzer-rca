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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;

// TODO: Scheme to rotate the current file and garbage collect older files.
public abstract class PersistorBase implements Persistable {
  private static final Logger LOG = LogManager.getLogger(PersistorBase.class);
  protected final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
  protected String dir;
  protected String filename;
  protected Connection conn;
  protected Set<String> tableNames;
  protected Date fileCreateTime;
  protected String filenameParam;
  protected String dbProtocol;
  private final int STORAGE_FILE_RETENTION_COUNT;
  private static final int STORAGE_FILE_RETENTION_COUNT_DEFAULT_VALUE = 5;
  private final File dirDB;

  private static final TimeUnit FILE_ROTATION_TIME_UNIT = TimeUnit.HOURS;
  private static final int ROTATION_PERIOD = 1;

  private final FileRotate fileRotate;
  private final FileGC fileGC;

  PersistorBase(String dir, String filename, String dbProtocolString, String storageFileRetentionCount) throws SQLException, IOException {
    this.dir = dir;
    this.filenameParam = filename;
    this.dbProtocol = dbProtocolString;

    this.dirDB = new File(this.dir);

    int parsedStorageFileRetentionCount;
    try {
      parsedStorageFileRetentionCount = Integer.parseInt(storageFileRetentionCount);
    } catch (NumberFormatException exp) {
      parsedStorageFileRetentionCount = STORAGE_FILE_RETENTION_COUNT_DEFAULT_VALUE;
      LOG.error(String.format("Unable to parse '%s' as integer", storageFileRetentionCount));
    }
    this.STORAGE_FILE_RETENTION_COUNT = parsedStorageFileRetentionCount;

    Path path = Paths.get(this.dirDB.getName(), filenameParam);
    fileRotate = new FileRotate(path, FILE_ROTATION_TIME_UNIT, ROTATION_PERIOD, dateFormat);
    fileRotate.forceRotate(System.currentTimeMillis());

    fileGC =  new FileGC(Paths.get(dir), filenameParam, FILE_ROTATION_TIME_UNIT, ROTATION_PERIOD,
            STORAGE_FILE_RETENTION_COUNT);
    openNewDBFile();
  }

  @Override
  public synchronized void close() throws SQLException {
    if (conn != null) {
      // conn.commit();
      conn.close();
    }
  }

  abstract void createTable(String tableName, List<Field<?>> columns);

  abstract void createTable(
      String tableName,
      List<Field<?>> columns,
      String refTable,
      String referenceTablePrimaryKeyFieldName);

  abstract int insertRow(String tableName, List<Object> columns);

  abstract String readTables();

  abstract RcaResponse readRcaTable(String rca);

  abstract void createNewDSLContext();

  // Not required for now.
  @Override
  public List<ResourceFlowUnit> read(Node<?> node) {
    return null;
  }

  public synchronized String read() {
    // Currently this method only contains a call to readTables() - later could add the part to read
    // multiple sqlite files
    LOG.debug("RCA: in read() in PersistorBase");
    String jsonResponse = readTables();
    return jsonResponse;
  }

  public synchronized RcaResponse readRca(String rca) {
    return readRcaTable(rca);
  }

  public synchronized void openNewDBFile() throws SQLException {
    this.fileCreateTime = new Date(System.currentTimeMillis());
    this.filename =
        String.format(
            "%s.%s",
            Paths.get(dir, filenameParam).toString(), dateFormat.format(this.fileCreateTime));
    this.tableNames = new HashSet<>();
    String url = String.format("%s%s", this.dbProtocol, this.filename);
    close();
    conn = DriverManager.getConnection(url);
    LOG.info("RCA: Periodic File Rotation - Created a new database connection - " + url);
    createNewDSLContext();
  }

  /**
   * This is used to persist a FlowUnit in the database.
   *
   * Before, we write anything the flowUnit is not empty and if we are past the rotation period,
   * then we rotate the database file and create a new one.
   * @param node Node whose flow unit is persisted. The graph node whose data is being written
   * @param flowUnit The flow unit that is persisted. The data taht will be persisted.
   * @param <T> The FlowUnit type
   * @throws SQLException A SQLException is thrown if we are unable to create a new connection
   * after the file rotation or while writing to the data base.
   * @throws IOException This is thrown if we are unable to delete the old database files.
   */
  @Override
  public synchronized <T extends ResourceFlowUnit> void write(Node<?> node, T flowUnit) throws SQLException, IOException {
    // Write only if there is data to be writen.
    if (flowUnit.isEmpty()) {
      LOG.debug("RCA: Flow unit isEmpty");
      return;
    }

    Path rotatedFile = fileRotate.tryRotate(System.currentTimeMillis());
    if (rotatedFile != null) {
      fileGC.eligibleForGc(rotatedFile.toFile().getName());
      openNewDBFile();
    }

    String tableName = node.getClass().getSimpleName();
    if (!tableNames.contains(tableName)) {
      LOG.info(
          "RCA: Table '{}' does not exist. Creating one with columns: {}",
          tableName,
          flowUnit.getSqlSchema());
      createTable(tableName, flowUnit.getSqlSchema());
      tableNames.add(tableName);
    }
    int lastPrimaryKey = insertRow(tableName, flowUnit.getSqlValue());

    if (flowUnit.hasResourceSummary()) {
      write(
          flowUnit.getResourceSummary(),
          tableName,
          getPrimaryKeyColumnName(tableName),
          lastPrimaryKey);
    }
  }

  /** recursively insert nested summary to sql tables */
  private synchronized void write(
      GenericSummary summary,
      String referenceTable,
      String referenceTablePrimaryKeyFieldName,
      int referenceTablePrimaryKeyFieldValue) {
    String tableName = summary.getClass().getSimpleName();
    if (!tableNames.contains(tableName)) {
      LOG.info(
          "RCA: Table '{}' does not exist. Creating one with columns: {}",
          tableName,
          summary.getSqlSchema());
      createTable(
          tableName, summary.getSqlSchema(), referenceTable, referenceTablePrimaryKeyFieldName);
      tableNames.add(tableName);
    }
    List<Object> values = summary.getSqlValue();
    values.add(Integer.valueOf(referenceTablePrimaryKeyFieldValue));
    int lastPrimaryKey = insertRow(tableName, values);
    for (GenericSummary nestedSummary : summary.getNestedSummaryList()) {
      write(nestedSummary, tableName, getPrimaryKeyColumnName(tableName), lastPrimaryKey);
    }
  }

  protected String getPrimaryKeyColumnName(String tableName) {
    return tableName + "_ID";
  }
}
