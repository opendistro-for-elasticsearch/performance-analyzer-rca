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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Consts.Table;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.PersistableAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistableObject;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistorBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.TableInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * A persistable object that persists all actionable actions published by Publisher
 * This object will be inserted as a tuple in "PublisherEvent" table. And any actionable actions will
 * be persisted separately in Action table and linked to this tuple via foreign key.
 * By joining one PublisherEvent with the Action table, we can map one decision(here it is PublisherEvent)
 * to a list of actions that were received and published as a batch from Collator
 *
 * <p>Table name : PublisherEvent
 *
 * <p>schema :
 * | ID(primary key) | timestamp | event_name | number_of_actions |
 * |      1          | 123       | Publisher  |       5           |
 */
public class PublisherEvent implements PersistableObject {
  private static final Logger LOG = LogManager.getLogger(PublisherEvent.class);
  private final String eventName;
  private final long timeStamp;
  private final int numOfActions;
  private final List<PersistableAction> actions;
  private static final List<Field<?>> schema;

  public PublisherEvent(String eventName, long timeStamp, List<PersistableAction> actions) {
    this.eventName = eventName;
    this.timeStamp = timeStamp;
    this.numOfActions = actions == null ? 0 : actions.size();
    this.actions = actions;
  }

  static {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(PublisherEventField.TIMESTAMP_FIELD.getField());
    fields.add(PublisherEventField.EVENT_FIELD.getField());
    fields.add(PublisherEventField.NUM_OF_ACTIONS_FIELD.getField());
    schema = Collections.unmodifiableList(fields);
  }

  public boolean isPersistable() {
    return numOfActions != 0;
  }

  @Override
  public String table() {
    return Table.PUBLISHER_EVENT_TABLE;
  }

  @Override
  public void write(PersistorBase persistorBase, TableInfo referenceTableInfo) throws SQLException {
    if (!persistorBase.hasTable(table())) {
      LOG.info("Action: Table '{}' does not exist. Creating one with schema: {}", table(), schema);
      persistorBase.createTable(table(), schema);
    }
    int lastPrimaryKey = persistorBase.insertRow(table(), getSqlValues());
    TableInfo tableInfo = new TableInfo(table(), lastPrimaryKey);
    for (PersistableObject action : actions) {
      action.write(persistorBase, tableInfo);
    }
  }

  private List<Object> getSqlValues() {
    List<Object> values = new ArrayList<>();
    values.add(timeStamp);
    values.add(eventName);
    values.add(numOfActions);
    return values;
  }

  public enum PublisherEventField implements JooqFieldValue {
    TIMESTAMP_FIELD(SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME, Long.class),
    EVENT_FIELD(SQL_SCHEMA_CONSTANTS.EVENT_COL_NAME, String.class),
    NUM_OF_ACTIONS_FIELD(SQL_SCHEMA_CONSTANTS.NUM_OF_ACTIONS_COL_NAME, String.class);

    private String name;
    private Class<?> clazz;
    PublisherEventField(final String name, Class<?> clazz) {
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

  public static class SQL_SCHEMA_CONSTANTS {

    public static final String TIMESTAMP_COL_NAME = "timestamp";
    public static final String EVENT_COL_NAME = "event_name";
    public static final String NUM_OF_ACTIONS_COL_NAME = "number_of_actions";
  }
}
