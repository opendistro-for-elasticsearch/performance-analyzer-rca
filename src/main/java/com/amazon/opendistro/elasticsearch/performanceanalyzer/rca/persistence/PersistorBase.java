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
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

// TODO: Scheme to rotate the current file and garbage collect older files.
public abstract class PersistorBase implements Persistable {
  private static final Logger LOG = LogManager.getLogger(PersistorBase.class);
  protected final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
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

  private final FileRotate fileRotate;
  private final FileGC fileGC;

  enum RotationType {
    TRY_ROTATE,
    FORCE_ROTATE
  }

  PersistorBase(String dir, String filename, String dbProtocolString,
                String storageFileRetentionCount, TimeUnit fileRotationTimeUnit,
                long fileRotationPeriod) throws SQLException, IOException {
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

    Path path = Paths.get(dir, filenameParam);
    fileRotate = new FileRotate(path, fileRotationTimeUnit, fileRotationPeriod, dateFormat);
    fileRotate.forceRotate(System.currentTimeMillis());

    fileGC =  new FileGC(Paths.get(dir), filenameParam, fileRotationTimeUnit, fileRotationPeriod,
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

  abstract void createTable(String tableName, List<Field<?>> columns) throws SQLException;

  abstract void createTable(
      String tableName,
      List<Field<?>> columns,
      String refTable,
      String referenceTablePrimaryKeyFieldName) throws SQLException;

  abstract int insertRow(String tableName, List<Object> columns) throws SQLException;

  abstract String readTables();

  abstract JsonElement readRca(String rca);

  abstract void createNewDSLContext();

  @VisibleForTesting
  public abstract Map<String, Result<Record>> getRecordsForAllTables();

  // Not required for now.
  @Override
  public synchronized List<ResourceFlowUnit> read(Node<?> node) {
    return null;
  }

  public synchronized String read() {
    // Currently this method only contains a call to readTables() - later could add the part to read
    // multiple sqlite files
    return readTables();
  }

  // TODO : readRca might return a list of RcaResponse if using range query
  //  The current API does not support range query because nobody is using it at this moment.
  //  we will revisit this function to decide what are the inputs of the new API which support range query and
  //  we might want to define a separate abstract method in interface to address that use case.
  @Override
  public synchronized JsonElement read(String rca) {
    JsonArray rcaJson = new JsonArray();
    JsonElement response = readRca(rca);
    if (response != null) {
      rcaJson.add(response);
    }
    return rcaJson;
  }

  private synchronized void openNewDBFile() throws SQLException {
    this.fileCreateTime = new Date(System.currentTimeMillis());
    this.filename = Paths.get(dir, filenameParam).toString();
    this.tableNames = new HashSet<>();
    String url = String.format("%s%s", this.dbProtocol, this.filename);
    close();
    conn = DriverManager.getConnection(url);
    createNewDSLContext();
  }

  /**
   * This is used to persist a FlowUnit in the database.
   *
   * <p>Before, we write anything the flowUnit is not empty and if we are past the rotation period,
   * then we rotate the database file and create a new one.
   * @param node Node whose flow unit is persisted. The graph node whose data is being written
   * @param flowUnit The flow unit that is persisted. The data taht will be persisted.
   * @param <T> The FlowUnit type
   * @throws SQLException A SQLException is thrown if we are unable to create a new connection
   *     after the file rotation or while writing to the data base.
   * @throws IOException This is thrown if we are unable to delete the old database files.
   */
  @Override
  public synchronized <T extends ResourceFlowUnit> void write(Node<?> node, T flowUnit) throws SQLException, IOException {
    // Write only if there is data to be writen.
    if (flowUnit.isEmpty()) {
      LOG.debug("RCA: Flow unit isEmpty");
      return;
    }

    rotateRegisterGarbageThenCreateNewDB(RotationType.TRY_ROTATE);

    try {
      writeFlowUnit(flowUnit, node.name());
    } catch (SQLException e) {
      LOG.error(
          "RCA: Multiple attempts to write the data for table '{}' failed", node.name(), e);
      // We rethrow this exception so that framework can take appropriate action.
      throw e;
    }
  }

  public synchronized <T> void write(T obj)
      throws SQLException, IOException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    Objects.requireNonNull(obj);
    rotateRegisterGarbageThenCreateNewDB(RotationType.TRY_ROTATE);
    try {
      writeImpl(obj);
    } catch (IllegalStateException | IllegalArgumentException | IllegalAccessException | InvocationTargetException | NoSuchMethodException
        illegalEx) {
      throw illegalEx;
    } catch (SQLException e) {
      LOG.info("RCA: Fail to write.", e);
      rotateRegisterGarbageThenCreateNewDB(RotationType.FORCE_ROTATE);
      try {
        writeImpl(obj);
      } catch (SQLException ex) {
        LOG.error("Failed to write multiple times. Giving up.");
        // We rethrow this exception so that framework can take appropriate action.
        throw e;
      }
    }
  }

  abstract <T> void writeImpl(T obj) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException;

  private synchronized void rotateRegisterGarbageThenCreateNewDB(RotationType type) throws IOException, SQLException {
    Path rotatedFile = null;
    long currTime = System.currentTimeMillis();
    switch (type) {
      case FORCE_ROTATE:
        rotatedFile = fileRotate.forceRotate(currTime);
        break;
      case TRY_ROTATE:
        rotatedFile = fileRotate.tryRotate(currTime);
        break;
    }
    if (rotatedFile != null) {
      fileGC.eligibleForGc(rotatedFile.toFile().getName());
    }

    // If we are here that means the tryRotate or the forceRotate didn't throw exception and therefore,
    // the current DBFile does not exist anymore. We therefore should create a new one.
    if (fileRotate.getLastRotatedMillis() == currTime) {
      openNewDBFile();
      LOG.info("Created a new DB file.");
    }
  }

  /**
   * Writing a flow unit can fail if the DB file does not exist or if it is corrupted. In such
   * cases, we create a new file and attempt to write the data in the new file.
   * @param flowUnit The flow unit to be persisted.
   * @param tableName The name of the table the data is to be persisted in.
   * @param <T> The Type of flowUnit.
   * @throws SQLException This is thrown when the DB files does not exist or the schema is
   *     corrupted.
   * @throws IOException This is thrown if the attempt to create a new DB file fails.
   */
  private synchronized <T extends ResourceFlowUnit> void writeFlowUnit(
      T flowUnit, String tableName) throws SQLException, IOException {
    try {
        tryWriteFlowUnit(flowUnit, tableName);
    } catch (SQLException | DataAccessException e) {
      LOG.info(
          "RCA: Fail to write to table '{}', creating a new DB file and retrying write/create operation", tableName, e);
      rotateRegisterGarbageThenCreateNewDB(RotationType.FORCE_ROTATE);
      tryWriteFlowUnit(flowUnit, tableName);
    }
  }

  private synchronized <T extends ResourceFlowUnit> void tryWriteFlowUnit(
          T flowUnit, String nodeName) throws SQLException, DataAccessException {
    String tableName = ResourceFlowUnit.RCA_TABLE_NAME;
    if (!tableNames.contains(tableName)) {
      LOG.info("RCA: Table '{}' does not exist. Creating one with columns: {}", tableName, flowUnit.getSqlSchema());
      createTable(tableName, flowUnit.getSqlSchema());
    }
    int lastPrimaryKey = insertRow(tableName, flowUnit.getSqlValue(nodeName));

    if (flowUnit.hasResourceSummary() && flowUnit.isSummaryPersistable()) {
      writeSummary(
              flowUnit.getPersistableSummary(),
              tableName,
              getPrimaryKeyColumnName(tableName),
              lastPrimaryKey);
    }
  }

  /** recursively insert nested summary to sql tables */
  private synchronized void writeSummary(
      GenericSummary summary,
      String referenceTable,
      String referenceTablePrimaryKeyFieldName,
      int referenceTablePrimaryKeyFieldValue) throws SQLException {
    String tableName = summary.getClass().getSimpleName();
    if (!tableNames.contains(tableName)) {
      LOG.info("RCA: Summary table '{}' does not exist. Creating one with columns: {}", tableName, summary.getSqlSchema());
      createTable(tableName, summary.getSqlSchema(), referenceTable, referenceTablePrimaryKeyFieldName);
    }
    List<Object> values = summary.getSqlValue();
    values.add(Integer.valueOf(referenceTablePrimaryKeyFieldValue));
    int lastPrimaryKey = insertRow(tableName, values);
    for (GenericSummary nestedSummary : summary.getNestedSummaryList()) {
      writeSummary(nestedSummary, tableName, getPrimaryKeyColumnName(tableName), lastPrimaryKey);
    }
  }

  protected String getPrimaryKeyColumnName(String tableName) {
    return tableName + "_ID";
  }
}
