/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Consts.Table;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.PersistableAction.SQL_SCHEMA_CONSTANTS.ActionField;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistableObject;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistorBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.TableInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * Actions that can be persisted in SQL
 *
 * <p>Table name : Action
 *
 * <p>schema :
 * | ID(primary key) |     action_name     | resource_type | resource_metric | node_id |
 * |      1          | ModifyQueueCapacity |      1        |       5         |  node1  |
 * | node_ip   | actionable | cool_off_period | current_value | desired_value | additional_json |
 * | 127.0.0.1 |  true      |  300            |   100         |  150          |                 |
 */
public abstract class PersistableAction extends SuppressibleAction implements PersistableObject {
  private static final Logger LOG = LogManager.getLogger(PersistableAction.class);
  private static final List<Field<?>> schema;
  private final Map<Field<?>, Object> rowValues;

  public PersistableAction(final AppContext appContext) {
    super(appContext);
    this.rowValues = new HashMap<>();
    initializeRowValues();
  }

  static {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(ActionField.ACTION_NAME_FIELD.getField());
    fields.add(ActionField.RESOURCE_TYPE_FIELD.getField());
    fields.add(ActionField.NODE_ID_FIELD.getField());
    fields.add(ActionField.NODE_IP_FIELD.getField());
    fields.add(ActionField.ACTIONABLE_FIELD.getField());
    fields.add(ActionField.COOL_OFF_PERIOD_FIELD.getField());
    fields.add(ActionField.CURRENT_VALUE_FIELD.getField());
    fields.add(ActionField.DESIRED_VALUE_FIELD.getField());
    fields.add(ActionField.ADDITIONAL_JSON_FIELD.getField());
    schema = Collections.unmodifiableList(fields);
  }

  private void initializeRowValues() {
    rowValues.put(ActionField.ACTION_NAME_FIELD.getField(), name());
    rowValues.put(ActionField.RESOURCE_TYPE_FIELD.getField(), -1);
    rowValues.put(ActionField.NODE_ID_FIELD.getField(), "unknown");
    rowValues.put(ActionField.NODE_IP_FIELD.getField(), "unknown");
    rowValues.put(ActionField.ACTIONABLE_FIELD.getField(), false);
    rowValues.put(ActionField.COOL_OFF_PERIOD_FIELD.getField(), -1);
    rowValues.put(ActionField.CURRENT_VALUE_FIELD.getField(), -1);
    rowValues.put(ActionField.DESIRED_VALUE_FIELD.getField(), -1);
    rowValues.put(ActionField.ADDITIONAL_JSON_FIELD.getField(), "");
  }

  protected void setRowValue(Field<?> field, Object object) {
    assert rowValues.containsKey(field) : "unrecognized field " + field.getName()
        + " in table " + table();
    this.rowValues.put(field, object);
  }

  private List<Object> getSqlValues() {
    List<Object> values = new ArrayList<>();
    schema.forEach(field -> values.add(rowValues.get(field)));
    return values;
  }

  @Override
  public String table() {
    return Table.ACTION_TABLE;
  }

  @Override
  public void write(PersistorBase persistorBase, TableInfo referenceTableInfo) throws SQLException {
    if (referenceTableInfo == null) {
      LOG.error("Action: Table {} is linked to a null reference table", table());
      throw new SQLException();
    }
    if (!persistorBase.hasTable(table())) {
      LOG.info("Action: Table '{}' does not exist. Creating one with schema: {}", table(), schema);
      persistorBase.createTable(table(), schema, referenceTableInfo.getTableName());
    }
    List<Object> values = getSqlValues();
    values.add(referenceTableInfo.getMostRecentPrimaryKey());
    persistorBase.insertRow(table(), getSqlValues());
  }

  public static class SQL_SCHEMA_CONSTANTS {
    public static final String ACTION_NAME_COL_NAME = "action_name";
    public static final String RESOURCE_TYPE_COL_NAME = "resource_type";
    public static final String NODE_ID_COL_NAME = "node_id";
    public static final String NODE_IP_COL_NAME = "node_ip";
    public static final String ACTIONABLE_COL_NAME = "actionable";
    public static final String COOL_OFF_PERIOD_COL_NAME = "cool_off_period";
    public static final String CURRENT_VALUE_COL_NAME = "current_value";
    public static final String DESIRED_VALUE_COL_NAME = "desired_value";
    public static final String ADDITIONAL_JSON_COL_NAME = "additional_json";

    /**
     * SQL fields
     */
    public enum ActionField implements JooqFieldValue {
      ACTION_NAME_FIELD(SQL_SCHEMA_CONSTANTS.ACTION_NAME_COL_NAME, String.class),
      RESOURCE_TYPE_FIELD(SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME, Integer.class),
      NODE_ID_FIELD(SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME, String.class),
      NODE_IP_FIELD(SQL_SCHEMA_CONSTANTS.NODE_IP_COL_NAME, String.class),
      ACTIONABLE_FIELD(SQL_SCHEMA_CONSTANTS.ACTIONABLE_COL_NAME, Boolean.class),
      COOL_OFF_PERIOD_FIELD(SQL_SCHEMA_CONSTANTS.COOL_OFF_PERIOD_COL_NAME, Long.class),
      CURRENT_VALUE_FIELD(SQL_SCHEMA_CONSTANTS.CURRENT_VALUE_COL_NAME, Long.class),
      DESIRED_VALUE_FIELD(SQL_SCHEMA_CONSTANTS.DESIRED_VALUE_COL_NAME, Long.class),
      ADDITIONAL_JSON_FIELD(SQL_SCHEMA_CONSTANTS.ADDITIONAL_JSON_COL_NAME, String.class);

      private String name;
      private Class<?> clazz;

      ActionField(final String name, Class<?> clazz) {
        this.name = name;
        this.clazz = clazz;
      }

      @Override
      public Field<?> getField() {
        return DSL.field(DSL.name(this.name), this.clazz);
      }

      @Override
      public String getName() {
        return this.name;
      }
    }
  }
}
