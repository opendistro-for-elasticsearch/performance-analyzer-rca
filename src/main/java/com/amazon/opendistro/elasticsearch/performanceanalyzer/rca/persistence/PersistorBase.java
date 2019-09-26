package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import java.io.File;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
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
  protected String dir;
  protected String filename;
  protected Connection conn;
  protected Set<String> tableNames;

  PersistorBase(String dir, String filename, String dbProtocolString) throws SQLException {
    this.dir = dir;
    this.filename =
        String.format("%s.%s", Paths.get(dir, filename).toString(), getFormattedCurrentTime());
    this.tableNames = new HashSet<>();
    String url = String.format("%s%s", dbProtocolString, this.filename);
    conn = DriverManager.getConnection(url);
    conn.setAutoCommit(false);
  }

  @Override
  public void close() throws SQLException {
    if (conn != null) {
      conn.close();
      renameFile();
    }
  }

  private void renameFile() {
    String newFileName = String.format("%s.%s", filename, getFormattedCurrentTime());
    if (!new File(filename).renameTo(new File(newFileName))) {
      // LOG.error("RCA: Error renaming file {} to {}", filename, newFileName);
    }
  }

  abstract void createTable(String tableName, List<String> columns);

  abstract void insertRow(String tableName, List<String> columns);

  // Not required for now.
  @Override
  public FlowUnit read(Node node) {
    return null;
  }

  // The database is always rotated when the thread dies. So the tablenames in the tableNames Set is
  // the
  // authoritative set for the current set of tables.
  // TODO: Add the code to rotate the table on exception and periodically
  //
  @Override
  public void write(Node node, FlowUnit flowUnit) {
    // Write only if there is data to be writen.
    if (flowUnit.isEmpty()) {
      LOG.info("RCA: Flow unit isEmpty");
      return;
    }
    String tableName = node.getClass().getSimpleName();
    // The first row of the flowUnit is the name of the columns.
    if (!tableNames.contains(tableName)) {
      List<String> columns = new ArrayList<>(flowUnit.getData().get(0));
      columns.add(RcaConsts.DATASTORE_CONTEXT_COL_NAME);
      columns.add(RcaConsts.DATASTORE_TIMESTAMP_COL_NAME);
      LOG.info("RCA: Table '{}' does not exist. Creating one with columns: {}", tableName, columns);
      createTable(tableName, columns);
      tableNames.add(tableName);
    }

    flowUnit.getData().stream()
        .skip(1)
        .forEach(
            row -> {
              List<String> data = new ArrayList<>(row);
              // Add the context string and the timestamp before inserting the row.
              data.add(flowUnit.getContextString());
              data.add(String.valueOf(flowUnit.getTimeStamp()));
              insertRow(tableName, data);
            });
  }

  private String getFormattedCurrentTime() {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    return df.format(new Date(System.currentTimeMillis()));
  }
}
