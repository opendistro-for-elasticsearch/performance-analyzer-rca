package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import java.sql.SQLException;

public interface Persistable {
  /**
   * @param node The data for the node to be read.
   * @return Returns a flow unit type
   */
  FlowUnit read(Node node);

  /**
   * Write data to the database.
   *
   * @param node Node whose flow unit is persisted.
   * @param flowUnit The flow unit that is persisted.
   */
  void write(Node node, FlowUnit flowUnit);

  void close() throws SQLException;
}
