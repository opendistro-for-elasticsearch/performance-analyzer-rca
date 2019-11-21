package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import java.sql.SQLException;
import java.util.List;

public interface Persistable {
  /**
   * @param node The data for the node to be read.
   * @return Returns a flow unit type
   */
  List<ResourceFlowUnit> read(Node node);

  String read();

  /**
   * Write data to the database.
   *
   * @param node Node whose flow unit is persisted.
   * @param flowUnit The flow unit that is persisted.
   */
  void write(Node node, ResourceFlowUnit flowUnit);

  void close() throws SQLException;
}
