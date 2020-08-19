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
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonElement;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.jooq.Record;
import org.jooq.Result;

public interface Persistable {
  /**
   * @param node The data for the node to be read.
   * @return Returns a flow unit type
   */
  List<ResourceFlowUnit> read(Node<?> node);

  /**
   * Read the raw RCA tables and print the
   * tables in json format
   * @return
   */
  String read();

  /**
   * Read the most recent status o a particular RCA from database
   * and convert the result into json
   * @param rca name of RCA to query
   * @return json result
   */
  JsonElement read(String rca);

  /**
   * Write data to the database.
   *
   * @param node Node whose flow unit is persisted.
   * @param flowUnit The flow unit that is persisted.
   */
  <T extends ResourceFlowUnit> void write(Node<?> node, T flowUnit) throws SQLException, IOException;

  void close() throws SQLException;

  /**
   * Get a list of all the distinct RCAs persisted in the current DB file.
   * @return A list of RCAs.
   */
  List<String> getAllPersistedRcas();

  /**
   * Get records for all the tables
   * @return A map of table and all the data contained in the table.
   */
  @VisibleForTesting
  Map<String, Result<Record>> getRecordsForAllTables();
}
