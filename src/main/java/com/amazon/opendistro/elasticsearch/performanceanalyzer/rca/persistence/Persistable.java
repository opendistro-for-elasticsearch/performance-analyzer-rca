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
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

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
   * This API reads the latest row from the table corresponding to the Object.
   * @param clz The Class whose Object is desired.
   * @param <T> The generic type of the class
   * @return An instantiated Object of the class with the fields populated with the data from the latest row in the table and other
   *     referenced tables or null if the table does not exist yet.
   * @throws NoSuchMethodException If the expected setter does not exist.
   * @throws IllegalAccessException If the setter is not Public
   * @throws InvocationTargetException If invoking the setter by reflection threw an exception.
   * @throws InstantiationException Creating an Object of the class failed for some reason.
   * @throws DataAccessException Thrown by the DB layer.
   */
  <T> @Nullable T read(Class<T> clz)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, DataAccessException;

  /**
   * Write data to the database.
   *
   * @param node Node whose flow unit is persisted.
   * @param flowUnit The flow unit that is persisted.
   */
  <T extends ResourceFlowUnit> void write(Node<?> node, T flowUnit) throws SQLException, IOException;

  /**
   * This API helps us write any Java Object into the Database and it does not need to be a Graph node anymore.
   * This is required because:
   * - in future we would like to persist 'remediation Actions' and they are not necessarily
   *    FlowUnits or Graph nodes.
   * - This removes de-coupling of the persistence layer from the RCA runtime.
   * Restrictions on the Object
   * --------------------------
   * The Object that can be persisted has certain restrictions:
   * - The Fields of the class that are to be persisted should be annotated with '@ValueColumn' or '@RefColumn'.
   *    A Field should be annotated as @ValueColumn if the field type is a Java primitive type or String type.
   *    The accepted types are boolean, byte, char, short, int, long, float, and double and their Boxed counterparts.
   *    A Column can be marked as @RefColumn if the class Field is an user defined Java Class or a collection of the same.
   * - The annotated Fields should have a 'getter' and a 'setter'. getters are expected to be named as 'get' or 'is'
   *     prepended to the _capitalized_ fieldName. And the setters are expected to be named 'set' and the capitalized
   *     field name. The type of the field should match the type of the getter's return type or the setter's argument
   *     type. Remember, int and Integer are two different types. Therefore, if the field is of type 'int' and your
   *     'getter' returns a value of type Integer, you will still get NoSuchMethodException.
   * - The annotated fields and the getter/setters should be declared in the Object and not in a Super class of it.
   * @param object The Object that needs to be persisted.
   * @param <T> The type of the Object.
   * @throws SQLException This is thrown if the underlying DB throws an error.
   * @throws IOException This is thrown in case the DB file rotation fails.
   * @throws IllegalAccessException If the getters and setters of the fields to be persisted are not Public.
   * @throws NoSuchMethodException If getter or setters don't exist with the expected naming convention.
   * @throws InvocationTargetException Invoking the getter or setter throws an exception.
   */
  <T> void write(@NonNull T object) throws SQLException, IOException, IllegalAccessException, NoSuchMethodException,
      InvocationTargetException;

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
