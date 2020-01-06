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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
  private static final int FILE_ROTATION_PERIOD_SECS = 3600;
  private static final int SQLITE_FILES_TO_KEEP = 5;

  PersistorBase(String dir, String filename, String dbProtocolString) throws SQLException {
    this.dir = dir;
    this.filenameParam = filename;
    this.dbProtocol = dbProtocolString;
    this.fileCreateTime = new Date(System.currentTimeMillis());
    this.filename =
        String.format(
            "%s.%s", Paths.get(dir, filename).toString(), dateFormat.format(this.fileCreateTime));
    this.tableNames = new HashSet<>();
    String url = String.format("%s%s", dbProtocolString, this.filename);
    conn = DriverManager.getConnection(url);
  }

  @Override
  public synchronized void close() throws SQLException {
    if (conn != null) {
      // conn.commit();
      conn.close();
    }
  }

  abstract void createTable(String tableName, List<String> columns);

  abstract void insertRow(String tableName, List<String> columns);

  abstract String readTables();

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

  public synchronized void rotateDBIfRequired() throws ParseException, SQLException {
    Date currTimeMs = new Date(System.currentTimeMillis());
    if ((currTimeMs.getTime() - this.fileCreateTime.getTime()) / 1000 > FILE_ROTATION_PERIOD_SECS) {
      openNewDBFile();
      deleteOldDBFile();
    }
  }

  public synchronized String getFilesInDirDB(String datePrefix) {
    File dirDB = new File(this.dir);
    File[] files =
        dirDB.listFiles(
            new FilenameFilter() {
              @Override
              public boolean accept(File dir, String name) {
                return name.startsWith(String.format("%s.%s", filenameParam, datePrefix));
              }
            });
    if (files.length == 0) {
      return "";
    }
    return files[0].toString();
  }

  public synchronized String getDBFilePath(int hours) throws ParseException {
    Date hoursBeforeFileCreateMs =
        new Date(this.fileCreateTime.getTime() - ((long) hours * 60 * 60 * 1000));
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH");
    String oldDBFilePath = getFilesInDirDB(df.format(hoursBeforeFileCreateMs));
    LOG.info("RCA: About to delete SQLite file - " + oldDBFilePath);
    return oldDBFilePath;
  }

  public synchronized void deleteOldDBFile()
      throws SQLException, SecurityException, ParseException {
    String oldDBFilePath = getDBFilePath(SQLITE_FILES_TO_KEEP);
    File dbFile = new File(oldDBFilePath);
    if (dbFile.exists()) {
      if (!dbFile.delete()) {
        LOG.error("Failed to delete File - " + oldDBFilePath);
      }
    }
    oldDBFilePath = getDBFilePath(SQLITE_FILES_TO_KEEP + 1);
    dbFile = new File(oldDBFilePath);
    if (dbFile.exists()) {
      if (!dbFile.delete()) {
        LOG.error("Failed to delete File - " + oldDBFilePath);
      }
    }
  }

  // The database is always rotated when the thread dies. So the tablenames in the tableNames Set is
  // the
  // authoritative set for the current set of tables.
  // TODO: Add the code to rotate the table on exception and periodically
  //
  @Override
  public synchronized <T extends ResourceFlowUnit> void write(Node<?> node, T flowUnit) {
    // Write only if there is data to be writen.
    if (flowUnit.isEmpty()) {
      LOG.info("RCA: Flow unit isEmpty");
      return;
    }
    String tableName = node.getClass().getSimpleName();

    try {
      rotateDBIfRequired();
    } catch (SQLException e) {
      LOG.error(
          "RCA: Caught SQLException while creating a new DB connection for file rotation.", e);
    } catch (ParseException e) {
      LOG.error("RCA: Caught ParseException while checking for file rotation.", e);
    } catch (SecurityException e) {
      LOG.error("RCA: Caught SecurityException while trying to delete old DB file. ", e);
    }

    // The first row of the flowUnit is the name of the columns.
    if (!tableNames.contains(tableName)) {
      List<String> columns = new ArrayList<>();
      columns.add(RcaConsts.DATASTORE_TIMESTAMP_COL_NAME);
      columns.add(RcaConsts.DATASTORE_RESOURCE_COL_NAME);
      columns.add(RcaConsts.DATASTORE_STATE_COL_NAME);
      columns.addAll(flowUnit.getData().get(0));
      LOG.info("RCA: Table '{}' does not exist. Creating one with columns: {}", tableName, columns);
      createTable(tableName, columns);
      tableNames.add(tableName);
    }

    flowUnit.getData().stream()
        // skip first row as it is supposed to be the column name in sql
        .skip(1)
        .forEach(
            row -> {
              List<String> data = new ArrayList<>();
              // Add the context string and the timestamp before inserting the row.
              data.add(String.valueOf(flowUnit.getTimeStamp()));
              data.add(flowUnit.getResourceContext().getResource().toString());
              data.add(flowUnit.getResourceContext().getState().toString());
              data.addAll(row);
              insertRow(tableName, data);
            });
  }
}
