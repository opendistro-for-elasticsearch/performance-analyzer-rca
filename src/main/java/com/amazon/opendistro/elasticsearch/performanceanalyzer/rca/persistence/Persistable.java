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
import java.sql.SQLException;
import java.util.List;

public interface Persistable {
  /**
   * @param node The data for the node to be read.
   * @return Returns a flow unit type
   */
  List<ResourceFlowUnit> read(Node<?> node);

  String read();

  /**
   * Write data to the database.
   *
   * @param node Node whose flow unit is persisted.
   * @param flowUnit The flow unit that is persisted.
   */
  <T extends ResourceFlowUnit> void write(Node<?> node, T flowUnit);

  void close() throws SQLException;
}
