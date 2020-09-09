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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit.ResourceFlowUnitFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.NodeLevelDimensionalSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.SQLiteQueryUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.NodeTemperatureRca;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.CreateTableConstraintStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStepN;
import org.jooq.JSONFormat;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

public class SQLitePersistor extends PersistorBase {
  private enum GetterOrSetter {
    GETTER,
    SETTER,
    NEITHER
  }

  private static class GetterSetterPairs {
    Method getter = null;
    Method setter = null;
  }

  private static class ColumnValuePair {
    Field<?> field;
    Object value;
  }

  private static final String DB_URL = "jdbc:sqlite:";
  private DSLContext create;
  private Map<String, List<Field<?>>> jooqTableColumns;
  private static final Logger LOG = LogManager.getLogger(SQLitePersistor.class);
  private static final String LAST_INSERT_ROWID = "last_insert_rowid()";
  private static final String PRIMARY_KEY_AUTOINCREMENT_POSTFIX = " INTEGER PRIMARY KEY AUTOINCREMENT";
  private Map<String, Class<?>> tableNameToJavaClassMap;

  private static final String[] GETTER_PREFIXES = {"get", "is"};
  private static final String[] SETTER_PREFIXES = {"set"};

  private static final String TABLE_NAME_JSON_KEY = "tableName";
  private static final String ROW_IDS_JSON_KEY = "rowIds";

  /**
   * This is for efficient lookup of the getter and setter methods for all the persistable Fields of a class. This map can be in-memory and
   * does not need to be re-created during DB file rotations as we don't support dynamic class loading and rotating the DB files should
   * not change the members of the class.
   */
  private Map<Class<?>, Map<java.lang.reflect.Field, GetterSetterPairs>> fieldGetterSetterPairsMap;

  private Map<Class<?>, Map<String, GetterSetterPairs>> classFieldNamesToGetterSetterMap;

  // When persisting an object in the DB, is a getter for the Object is annotated with @AColumn and @ATable, then the return type Object is
  // persisted in a a different table and the primary key of the other table is persisted as a pointer in the outer object table.
  private static final String NESTED_OBJECT_COLUMN_PREFIX = "__table__";

  private static int id_test = 1;

  @VisibleForTesting
  public SQLitePersistor(String dir, String filename, String storageFileRetentionCount,
                  TimeUnit rotationTime, long rotationPeriod) throws SQLException, IOException {
    super(dir, filename, DB_URL, storageFileRetentionCount, rotationTime, rotationPeriod);
    create = DSL.using(conn, SQLDialect.SQLITE);
    jooqTableColumns = new HashMap<>();
    tableNameToJavaClassMap = new HashMap<>();
    this.fieldGetterSetterPairsMap = new HashMap<>();
    this.classFieldNamesToGetterSetterMap = new HashMap<>();
  }

  // This updates the DSL context based on a new SQLite connection
  // It is needed during SQLite file rotation
  @Override
  synchronized void createNewDSLContext() {
    if (create != null) {
      create.close();
    }
    create = DSL.using(super.conn, SQLDialect.SQLITE);
    jooqTableColumns = new HashMap<>();
    tableNameToJavaClassMap = new HashMap<>();
  }

  @Override
  synchronized void createTable(String tableName, List<Field<?>> columns) throws SQLException {
    CreateTableConstraintStep constraintStep = create.createTable(tableName)
        //sqlite does not support identity. use plain sql string instead.
        .column(DSL.field(getPrimaryKeyColumnName(tableName) + PRIMARY_KEY_AUTOINCREMENT_POSTFIX))
        .columns(columns);

    try {
      constraintStep.execute();
      LOG.debug("Successfully created table: {}", tableName);
    } catch (DataAccessException ex) {
      String msg = "table " + tableName + " already exists";
      if (ex.getMessage().contains(msg)) {
        LOG.debug(ex.getMessage());
      } else {
        LOG.error(ex);
        throw new SQLException(ex);
      }
    }
    tableNames.add(tableName);
    jooqTableColumns.put(tableName, columns);
    LOG.debug("Added table '{}' and its columns: '{}' to in-memory registry.", tableName, columns);
  }

  /**
   * create table with foreign key
   */
  @Override
  synchronized void createTable(String tableName, List<Field<?>> columns, String referenceTableName,
                                String referenceTablePrimaryKeyFieldName) throws SQLException {
    Field foreignKeyField = DSL.field(referenceTablePrimaryKeyFieldName, Integer.class);
    columns.add(foreignKeyField);

    try {
      LOG.debug("Trying to create a summary table: {} that references {}", tableName, referenceTableName);
      Table referenceTable = DSL.table(referenceTableName);
      CreateTableConstraintStep constraintStep = create.createTable(tableName)
          .column(DSL.field(getPrimaryKeyColumnName(tableName) + PRIMARY_KEY_AUTOINCREMENT_POSTFIX))
          .columns(columns)
          .constraints(DSL.constraint(foreignKeyField.getName() + "_FK").foreignKey(foreignKeyField)
              .references(referenceTable, DSL.field(referenceTablePrimaryKeyFieldName)));
      constraintStep.execute();
      LOG.debug("table with fk created: {}", constraintStep.toString());
    } catch (DataAccessException e) {
      String msg = "table " + tableName + " already exists";
      if (e.getMessage().contains(msg)) {
        LOG.debug(e.getMessage());
      } else {
        LOG.error("Error creating table: {}", tableName, e);
        throw new SQLException(e);
      }
    } catch (Exception ex) {
      LOG.error(ex);
      throw new SQLException(ex);
    }
    tableNames.add(tableName);
    jooqTableColumns.put(tableName, columns);
  }

  @Override
  synchronized int insertRow(String tableName, List<Object> row) throws SQLException {
    int lastPrimaryKey = -1;
    String sqlQuery = "SELECT " + LAST_INSERT_ROWID;

    Objects.requireNonNull(create, "DSLContext cannot be null");
    Table<Record> table = DSL.table(tableName);
    List<Field<?>> columnsForTable = jooqTableColumns.get(tableName);
    if (columnsForTable == null) {
      LOG.error("NO columns found for table: {}. Tables: {}, columns: {}", tableName, tableNames, jooqTableColumns);
      throw new SQLException("No columns exist for table.");
    }

    InsertValuesStepN insertValuesStepN = create
        .insertInto(table)
        .columns(columnsForTable)
        .values(row);

    try {
      insertValuesStepN.execute();
      LOG.debug("sql insert: {}", insertValuesStepN.toString());
      lastPrimaryKey = create.fetch(sqlQuery).get(0).get(LAST_INSERT_ROWID, Integer.class);
    } catch (Exception e) {
      LOG.error("Failed to insert into the table {}", tableName, e);
      throw new SQLException(e);
    }
    LOG.debug("most recently inserted primary key = {}", lastPrimaryKey);
    return lastPrimaryKey;
  }

  @Override
  public synchronized <T> @org.checkerframework.checker.nullness.qual.Nullable T read(Class<T> clz)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, DataAccessException {
    return read(clz, -1 /* To indicate this is the top level call */);
  }

  public synchronized <T> @org.checkerframework.checker.nullness.qual.Nullable T read(Class<T> clz, int rowId)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    String tableName = getTableNameFromClassName(clz);
    String primaryKeyCol = SQLiteQueryUtils.getPrimaryKeyColumnName(tableName);
    Field<Integer> primaryKeyField = DSL.field(primaryKeyCol, Integer.class);

    Map<String, GetterSetterPairs> fieldNameToGetterSetterMap = classFieldNamesToGetterSetterMap.get(clz);

    List<Record> recordList;
    if (rowId == -1) {
      try {
        // Fetch the latest row.
        recordList = create.select().from(tableName).orderBy(primaryKeyField.desc()).limit(1).fetch();
      } catch (DataAccessException dex) {
        LOG.debug("Error querying table {}", tableName, dex);
        return null;
      }
    } else {
      try {
        // get the row with the provided rowId.
        recordList = create.select().from(tableName).where(DSL.field(primaryKeyCol, Integer.class).eq(rowId)).fetch();
      } catch (DataAccessException dex) {
        // This is more severe. This would mean that the data corresponding to the outer Object were found but
        // the nested tables could not be accessed.
        LOG.error("Could not find data for table {}", tableName, dex);
        throw dex;
      }
    }

    if (recordList.size() != 1) {
      // We always expect one row whether we query for the latest row or we query for a row by the rowID.
      throw new IllegalStateException("Expected one row, found: '" + recordList + "'");
    }
    Record record = recordList.get(0);
    Field<?>[] fields = record.fields();
    T obj = clz.getDeclaredConstructor().newInstance();

    for (Field<?> jooqField : fields) {
      String columnName = jooqField.getName();
      if (columnName.equals(primaryKeyCol)) {
        continue;
      }

      if (columnName.startsWith(NESTED_OBJECT_COLUMN_PREFIX)) {
        // If the name of the column starts with the prefix, then it is a reference Column. It means that the value contained in this
        // column references to a particular RowIDs in the table with name same as this column(with prefix removed).
        // There are two forms of this:
        // - It can be a reference to another table, in which case the column is an integer type.
        // - OR it is a collection of RowIDs from multiple tables, in which case the column type a JsonArray as String. e.g.
        //    [{\"tableName\":\"ITestImpl2\",\"rowIds\":[1]},{\"tableName\":\"ITestImpl1\",\"rowIds\":[1,2]}]
        String nestedTableName = columnName.replace(NESTED_OBJECT_COLUMN_PREFIX, "");
        if (jooqField.getType() == String.class) {
          String value = (String) jooqField.getValue(record);
          JsonParser valueParser = new JsonParser();
          JsonArray array = valueParser.parse(value).getAsJsonArray();
          Method setter = fieldNameToGetterSetterMap.get(nestedTableName).setter;

          List<Object> collection = new ArrayList<>();
          for (JsonElement element: array) {
            JsonObject jsonObject = element.getAsJsonObject();
            String actualTableName = jsonObject.get(TABLE_NAME_JSON_KEY).getAsString();

            Class<?> actualTableClass = tableNameToJavaClassMap.get(actualTableName);
            if (actualTableClass == null) {
              throw new IllegalStateException("The table name '" + actualTableName + "' does not exist in the table to class mapping. But"
                  + "the database row mentions it: " + element.toString());
            }

            for (JsonElement rowIdElem: jsonObject.get(ROW_IDS_JSON_KEY).getAsJsonArray()) {
              int rowIdNestedTable = rowIdElem.getAsInt();
              Object nestedObj = read(actualTableClass, rowIdNestedTable);
              collection.add(nestedObj);
            }
          }
          setter.invoke(obj, collection);
        } else if (jooqField.getType() == Integer.class) {
          // This references a row in a table.
          if (fieldNameToGetterSetterMap.get(nestedTableName) == null) {
            throw new IllegalStateException("No Field Mapping exist for column name " + jooqField.getName() + " of table " + tableName);
          }
          Method setter = fieldNameToGetterSetterMap.get(nestedTableName).setter;

          if (setter.getParameterTypes().length != 1) {
            throw new IllegalStateException("A setter " + setter.getName() + " of class " + clz.getSimpleName()
                + " accepts more than one arguments.");
          }

          // This gives the type of the setter parameter.
          Class<?> setterType = setter.getParameterTypes()[0];
          int nestedRowId = jooqField.cast(Integer.class).getValue(record);

          // Now that we have the Type of the parameter and the rowID specifying the data the object
          // is to be filled with; we call the read method recursively to create the referenced Object
          // and then invoke the setter with it.
          Object nestedObj = read(setterType, nestedRowId);
          setter.invoke(obj, nestedObj);
        }
        else {
          throw new IllegalStateException("ReferenceColumn can be either Integer or String.");
        }
      } else {
        // For all the other columns, we look for the corresponding setter.
        Method setter = fieldNameToGetterSetterMap.get(jooqField.getName()).setter;
        setter.invoke(obj, jooqField.getType().cast(jooqField.getValue(record)));
      }
    }
    return obj;
  }

  synchronized <T> void writeImpl(T obj)
      throws IllegalStateException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SQLException,
      IllegalAccessException {
    writeImplInner(obj);
  }

  private static String getTableNameFromClassName(Class<?> clz) {
    return clz.getSimpleName();
  }

  private Class<?> getGenericParamTypeOfMethodReturn(Method method) {
    ParameterizedType mtype = (ParameterizedType) method.getGenericReturnType();
    return getFirstTypeFromParameterizedTypes(mtype, method.getName());
  }

  private Class<?> getGenericFieldType(java.lang.reflect.Field field) {
    ParameterizedType mtype = (ParameterizedType) field.getGenericType();
    return getFirstTypeFromParameterizedTypes(mtype, field.getName());
  }

  private Class<?> getFirstTypeFromParameterizedTypes(ParameterizedType mtype, String name) {
    Type[] mTypeArguments = mtype.getActualTypeArguments();
    if (mTypeArguments.length != 1) {
      throw new IllegalStateException("Expected list of a single type. Please check field/method: " + name);
    }
    Class mTypeArgClass = (Class) mTypeArguments[0];
    return mTypeArgClass;
  }

  private void checkPublic(Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      throw new IllegalStateException("Found '" + method.getName() + "'. But it is not public.");
    }
  }

  private String capitalize(String name) {
    if (name.length() == 1) {
      return name.toUpperCase();
    }
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }

  private void checkValidType(java.lang.reflect.Field field, Class<?> clz) {
    Type type = field.getGenericType();
    Annotation[] annotation = field.getAnnotations();
    StringBuilder err = new StringBuilder();
    err.append("[class: ").append(clz.getSimpleName()).append(":field:").append(field.getName()).append("]::");

    if (annotation.length != 1) {
      err
          .append("A field can have either ")
          .append(ValueColumn.class.getSimpleName())
          .append(" or ")
          .append(RefColumn.class.getSimpleName())
          .append(" not both");
      throw new IllegalStateException(err.toString());
    }

    if (annotation[0].annotationType() == RefColumn.class) {
      if (type instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Class<?> actualType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
        if (actualType.isPrimitive() || actualType == String.class) {
          err.append("RefColumn can only be used for a user defined class or a Collection of user-defined class NOT "
              + "a primitive or String type");
          throw new IllegalStateException(err.toString());
        }
      } else {
        Class<?> justType = field.getType();
        if (justType.isPrimitive() || justType == String.class) {
          err.append("RefColumn can only be used for a user defined class or a Collection of user-defined class NOT "
              + "a primitive or String type");
          throw new IllegalStateException(err.toString());
        }
      }
    } else {
      if (type instanceof ParameterizedType) {
        // TODO: This will be added if use-cases require this.
        err.append("persisting Primitives or Strings as Parameterized Types is not supported.");
        throw new IllegalStateException(err.toString());
      }
    }
  }

  /**
   * Go over all the fields of the class and then filter out all that are annotated as @AColumn or @ATable. For those fields,
   * try to figure out the getter and setters.
   * @param clz The class whose field registry is to be created.
   * @param <T> The Generic type of the class.
   * @throws IllegalStateException When getters and setters are not found for the field that is required to be persisted or they exist but
   *     are not public.
   */
  private <T> void createFieldRegistry(Class<T> clz) throws IllegalStateException, NoSuchMethodException {
    fieldGetterSetterPairsMap.putIfAbsent(clz, new HashMap<>());
    classFieldNamesToGetterSetterMap.putIfAbsent(clz, new HashMap<>());

    Map<java.lang.reflect.Field, GetterSetterPairs> fieldToGetterSetterMap = fieldGetterSetterPairsMap.get(clz);
    Map<String, GetterSetterPairs> fieldNameToGetterSetterMap = classFieldNamesToGetterSetterMap.get(clz);

    for (java.lang.reflect.Field field : clz.getDeclaredFields()) {
      if (field.isAnnotationPresent(ValueColumn.class) || field.isAnnotationPresent(RefColumn.class)) {
        checkValidType(field, clz);
        // Now we try to find the corresponding Getter and Setter for this field.
        GetterSetterPairs pair = new GetterSetterPairs();

        String capitalizedFieldName = capitalize(field.getName());
        for (String prefix: GETTER_PREFIXES) {
          String key = prefix + capitalizedFieldName;
          Method method;
          try {
            method = clz.getDeclaredMethod(key);
          } catch (NoSuchMethodException nom) {
            continue;
          }
          if (method.getReturnType() != field.getType()) {
            StringBuilder sb = new StringBuilder("The return type of the getter '");
            sb.append(key)
                .append("' (")
                .append(method.getReturnType())
                .append(") and field '")
                .append(field.getName())
                .append("' (")
                .append(field.getType())
                .append(") don't match.");
            throw new NoSuchMethodException(sb.toString());
          }
          checkPublic(method);
          pair.getter = method;
          break;
        }
        for (String prefix: SETTER_PREFIXES) {
          String key = prefix + capitalizedFieldName;
          try {
            // This line will throw if no method with the name exists or if a method with such name exists
            // but the method argument types are not the same. Remember, int and Integer are not the same
            // types.
            Method method = clz.getDeclaredMethod(key, field.getType());
            checkPublic(method);
            pair.setter = method;
            break;
          } catch (NoSuchMethodException e) {
          }
        }
        if (pair.getter == null) {
          throw new NoSuchMethodException(getNoGetterSetterExist(clz, field, GetterOrSetter.GETTER));
        }
        if (pair.setter == null) {
          throw new NoSuchMethodException(getNoGetterSetterExist(clz, field, GetterOrSetter.SETTER));
        }
        fieldToGetterSetterMap.put(field, pair);
        fieldNameToGetterSetterMap.put(field.getName(), pair);
      }
    }
  }

  private String getNoGetterSetterExist(Class<?> clz, java.lang.reflect.Field field, GetterOrSetter getterOrSetter) {
    String type;
    switch (getterOrSetter) {
      case GETTER:
        type = "getter";
        break;
      case SETTER:
        type = "setter";
        break;
      default:
        throw new IllegalArgumentException("Unrecognized type: " + getterOrSetter);
    }

    StringBuilder sb = new StringBuilder("Could not find '");
    sb.append(type)
        .append("' for the field '")
        .append(field.getName())
        .append("' of class '")
        .append(clz.getName())
        .append("'. Getters are expected to start with 'get' or 'is' and setters are expected to start with 'set' and they are required to"
        + " end with the name of the field (case insensitive.)");
    return sb.toString();
  }

  private <T> ColumnValuePair writeCollectionReferenceColumn(java.lang.reflect.Field field, Method getter, T obj)
      throws InvocationTargetException, IllegalAccessException, SQLException, NoSuchMethodException {
    ColumnValuePair columnValuePair = new ColumnValuePair();
    String columnName = NESTED_OBJECT_COLUMN_PREFIX + field.getName();

    Collection<?> collection = (Collection<?>) getter.getReturnType().cast(getter.invoke(obj));
    Map<String, List<Integer>> nestedPrimaryKeys = new HashMap<>();
    for (Object o: collection) {
      String myActualType = o.getClass().getSimpleName();
      nestedPrimaryKeys.putIfAbsent(myActualType, new ArrayList<>());

      Class typeArgClass = getGenericParamTypeOfMethodReturn(getter);

      int id = writeImplInner(typeArgClass.cast(o));
      nestedPrimaryKeys.get(myActualType).add(id);
    }
    JsonArray json = new JsonArray();
    // Create fields with the collectionReferenceType columns
    for (Map.Entry<String, List<Integer>> colNameEntry: nestedPrimaryKeys.entrySet()) {
      JsonObject jsonObject = new JsonObject();
      JsonArray jsonArrayInner = new JsonArray();
      colNameEntry.getValue().forEach(rowId -> jsonArrayInner.add(rowId));

      jsonObject.addProperty(TABLE_NAME_JSON_KEY, colNameEntry.getKey());
      jsonObject.add(ROW_IDS_JSON_KEY, jsonArrayInner);

      json.add(jsonObject);
    }
    columnValuePair.field = (DSL.field(DSL.name(columnName), String.class));
    columnValuePair.value = json.toString();
    return columnValuePair;
  }

  private <T> int writeImplInner(T obj)
      throws IllegalStateException, IllegalAccessException, InvocationTargetException, SQLException, NoSuchMethodException {
    Class<?> clz = obj.getClass();
    String tableName = getTableNameFromClassName(clz);
    Table<Record> table = DSL.table(tableName);

    // If there exists a table with the same name as the SimpleName of this class, make sure that the persisted class is same as this.
    // This is to avoid cases of trying to persist classes from two different packages with the same name.
    if (jooqTableColumns.containsKey(tableName)) {
      // There can be a case where two distinct classes with the same SimpleName, might want
      // to persist themselves. In this case, we should keep things simple and throw an error.
      Class<?> alreadyStoredTableClass = tableNameToJavaClassMap.get(tableName);
      Objects.requireNonNull(alreadyStoredTableClass, "A table exists with this name but the table is not mapped to a Java class.");
      if (alreadyStoredTableClass != clz) {
        throw new IllegalStateException("There is already a table in the Database with the same name. It belongs to the class: '"
            + alreadyStoredTableClass + "'. Please consider re-naming your classes.");
      }
      Objects.requireNonNull(fieldGetterSetterPairsMap.get(clz), "Because the class is already persisted once, we should have the "
          + "mapping for field to their corresponding getter and setters.");
    } else {
      createFieldRegistry(clz);
    }

    Map<java.lang.reflect.Field, GetterSetterPairs> fieldToGetterSetterMap = fieldGetterSetterPairsMap.get(clz);
    List<Field<?>> fields = new ArrayList<>();
    List<Object> values = new ArrayList<>();

    for (Map.Entry<java.lang.reflect.Field, GetterSetterPairs> entry: fieldToGetterSetterMap.entrySet()) {
      Method getter = entry.getValue().getter;
      java.lang.reflect.Field classField = entry.getKey();

      String columnName = classField.getName();
      Class<?> retType = getter.getReturnType();

      if (classField.isAnnotationPresent(RefColumn.class)) {
        columnName = NESTED_OBJECT_COLUMN_PREFIX + columnName;
        if (Collection.class.isAssignableFrom(retType)) {
          ColumnValuePair columnValuePair = writeCollectionReferenceColumn(classField, getter, obj);
          fields.add(columnValuePair.field);
          values.add(columnValuePair.value);
        } else {
          // This is a user-defined class Type
          int id = writeImplInner(retType.cast(getter.invoke(obj)));
          // Although the ID is long, we are persisting it as string because if there are multiple rows in the child table, that refer to
          // the parent table row, then, the parent table should have a list of them. IN which case the value stored in the column will be
          // of the form: [id1, id2, ..].
          fields.add(DSL.field(DSL.name(columnName), Integer.class));
          values.add(id);
        }
      } else if (retType.isPrimitive()) {
        fields.add(DSL.field(DSL.name(columnName), retType));
        values.add(getter.invoke(obj));
      } else if (retType == String.class) {
        fields.add(DSL.field(DSL.name(columnName), String.class));
        values.add(getter.invoke(obj));
      }
    }

    if (fields.size() == 0) {
      StringBuilder sb = new StringBuilder();
      sb.append("Class ")
          .append(clz.getSimpleName())
          .append(" was asked to be persisted but there are no fields with annotations: ")
          .append(ValueColumn.class.getSimpleName())
          .append(" or ")
          .append(RefColumn.class.getSimpleName());
      throw new IllegalStateException(sb.toString());
    }

    // If table does not exist, try to create one.
    if (!jooqTableColumns.containsKey(tableName)) {
      createTable(tableName, fields);
      tableNameToJavaClassMap.put(tableName, obj.getClass());
    }

    try {
      int ret = create.insertInto(table).columns(fields).values(values).execute();
    } catch (Exception e) {
      LOG.error("Inserting row '{}' into table '{}' failed", values, tableName, e);
      throw new SQLException(e);
    }
    int lastRowId = -1;
    String sqlQuery = "SELECT " + LAST_INSERT_ROWID;

    try {
      lastRowId = create.fetch(sqlQuery).get(0).get(LAST_INSERT_ROWID, Integer.class);
    } catch (Exception e) {
      LOG.error("Failed to insert into the table {}", table, e);
      throw new SQLException(e);
    }
    LOG.debug("most recently inserted primary key = {}", lastRowId);
    return lastRowId;
  }

  // This reads all SQLite tables in the latest SQLite file and converts the read data to JSON.
  @Override
  synchronized String readTables() {
    JsonParser jsonParser = new JsonParser();
    JsonObject tablesObject = new JsonObject();
    super.tableNames.forEach(
        table -> {
          String tableStr = readTable(table);
          try {
            JsonElement tableElement = jsonParser.parse(tableStr);
            tablesObject.add(table, tableElement);
          } catch (JsonSyntaxException se) {
            LOG.error("RCA: Json parsing fails when reading from table {}", table);
          }
        }
    );
    return tablesObject.toString();
  }

  @VisibleForTesting
  public synchronized Map<String, Result<Record>> getRecordsForAllTables() {
    Map<String, Result<Record>> results = new HashMap<>();
    super.tableNames.forEach(
            table -> results.put(table, getRecords(table))
    );
    return results;
  }

  @Override
  public synchronized List<String> getAllPersistedRcas() {
    List<String> uniquePersistedRcas = new ArrayList<>();
    try {
          uniquePersistedRcas =
                  (List<String>) create.selectDistinct(ResourceFlowUnitFieldValue.RCA_NAME_FILELD.getField())
          .from(ResourceFlowUnit.RCA_TABLE_NAME)
          .fetch(0).stream().collect(Collectors.toList());
    } catch (DataAccessException dex) {

    }
    return uniquePersistedRcas;
  }

  //read table content and convert it into JSON format
  private synchronized String readTable(String tableName) {
    String tableStr;
    try {
      Result<Record> result;
      if (tableName.equals(ResourceFlowUnit.RCA_TABLE_NAME)) {
        result = create.select()
            .from(tableName)
            .orderBy(ResourceFlowUnitFieldValue.RCA_NAME_FILELD.getField())
            .fetch();
      } else {
        result = create.select().from(tableName).fetch();
      }
      tableStr = result.formatJSON(new JSONFormat().header(false));
    } catch (DataAccessException e) {
      LOG.error("Fail to read table {}", tableName);
      tableStr = "[]";
    }
    return tableStr;
  }

  private synchronized @Nullable Result<Record> getRecords(String tableName) {
    try {
      Result<Record> result;
      if (tableName.equals(ResourceFlowUnit.RCA_TABLE_NAME)) {
        result = create.select()
                .from(tableName)
                .orderBy(ResourceFlowUnitFieldValue.RCA_NAME_FILELD.getField())
                .fetch();
      } else {
        result = create.select().from(tableName).fetch();
      }
      return result;
    } catch (DataAccessException e) {
      LOG.error("Fail to read table {}", tableName);
    }
    return null;
  }

  /**
   * FullTemperatureSummary is not one single Rca, instead it is a conglomeration of
   * temperature across all dimensions. Therefore, it iterates over all the dimensional tables
   * to arrive at that result.
   *
   * @return Returns a JsonObject with full temperature profile.
   */
  private JsonElement constructFullTemperatureProfile() {
    JsonObject rcaResponseJson = null;
    JsonArray nodeDimensionalSummary = null;
    String summaryName = NodeLevelDimensionalSummary.SUMMARY_TABLE_NAME;

    // We use the JsonObject returned as part of the first dimension as the template and then
    // for each subsequent dimension, we extend the json Array we have from the first dimension.
    for (String dimension : SQLiteQueryUtils.temperatureProfileDimensionRCASet) {
      if (rcaResponseJson == null) {
        rcaResponseJson = readTemperatureProfileRca(dimension).getAsJsonObject();
        JsonElement elem = rcaResponseJson.get(summaryName);
        if (elem == null) {
          rcaResponseJson = null;
          continue;
        }
        nodeDimensionalSummary = rcaResponseJson.get(summaryName).getAsJsonArray();
        nodeDimensionalSummary.get(0).getAsJsonObject().addProperty(
            ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME,
            rcaResponseJson.get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME).getAsString());

      } else {
        JsonObject resp = readTemperatureProfileRca(dimension).getAsJsonObject();
        if (resp != null && resp.getAsJsonObject().get(summaryName) != null) {
          JsonObject obj =
              resp.getAsJsonObject().get(summaryName).getAsJsonArray().get(0).getAsJsonObject();
          obj.addProperty(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME,
              resp.get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME).getAsString());
          nodeDimensionalSummary.add(obj);
        }
      }
    }

    if (rcaResponseJson != null) {
      // This is the name of the first dimension ans hence confusing. This element is redundant
      // anyways as the top level object is the name of the RCA queried for.
      rcaResponseJson.remove(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.RCA_COL_NAME);

      // State of a temperature profile RCA has no meaning. SO we remove this.
      rcaResponseJson.remove(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME);

      // Timestamp field exists for each dimension. A top level timestamp is not required.
      rcaResponseJson.remove(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME);
    }
    return rcaResponseJson;
  }

  private synchronized void readSummary(GenericSummary upperLevelSummary, int upperLevelPrimaryKey) {
    String upperLevelTable = upperLevelSummary.getTableName();

    // stop the recursion here if the summary does not have any nested summary table.
    if (upperLevelSummary.getNestedSummaryTables() == null) {
      return;
    }

    for (String nestedTableName : upperLevelSummary.getNestedSummaryTables()) {
      Field<Integer> foreignKeyField = DSL.field(
          SQLiteQueryUtils.getPrimaryKeyColumnName(upperLevelTable), Integer.class);
      SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils
          .buildSummaryQuery(create, nestedTableName, upperLevelPrimaryKey, foreignKeyField);
      try {
        Result<Record> recordList = rcaQuery.fetch();
        for (Record record : recordList) {
          GenericSummary summary = upperLevelSummary.buildNestedSummary(nestedTableName, record);
          if (summary != null) {
            Field<Integer> primaryKeyField = DSL.field(
                SQLiteQueryUtils.getPrimaryKeyColumnName(summary.getTableName()), Integer.class);
            readSummary(summary, record.get(primaryKeyField));
          }
        }
      } catch (DataAccessException de) {
        // it is totally fine if we fail to read some certain tables as some types of summaries might be missing
        LOG.warn("Fail to read Summary table : {}, query = {}", nestedTableName, rcaQuery.toString(), de);
      } catch (IllegalArgumentException ie) {
        LOG.error("Reading nested summary from wrong table, message : {}", ie.getMessage());
      }
    }
  }

  private synchronized JsonElement getTemperatureRca(String rca) {
    JsonElement temperatureRcaJson;
    switch (rca) {
      case SQLiteQueryUtils.ALL_TEMPERATURE_DIMENSIONS:
        temperatureRcaJson = constructFullTemperatureProfile();
        break;
      default:
        temperatureRcaJson = readTemperatureProfileRca(rca);
    }
    return temperatureRcaJson;
  }

  private synchronized JsonElement getNonTemperatureRcas(String rca) {
    RcaResponse response = null;
    Field<Integer> primaryKeyField = DSL.field(
        SQLiteQueryUtils.getPrimaryKeyColumnName(ResourceFlowUnit.RCA_TABLE_NAME), Integer.class);
    SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils.buildRcaQuery(create, rca);
    try {
      List<Record> recordList = rcaQuery.fetch();
      if (recordList.size() > 0) {
        Record mostRecentRecord = recordList.get(0);
        response = RcaResponse.buildResponse(mostRecentRecord);
        if (response.getState().equals(State.UNHEALTHY.toString())) {
          readSummary(response, mostRecentRecord.get(primaryKeyField));
        }
      }
    } catch (DataAccessException de) {
      if (!de.getMessage().contains("no such table")) {
        // it is totally fine if we fail to read some certain tables.
        LOG.error("Fail to read RCA : {}.", rca, de);
      }
    }
    JsonElement ret = null;
    if (response != null) {
      ret = response.toJson();
    }
    return ret;
  }

  // TODO: we only query the most recent RCA entry in this API. might need to extend this
  // to support range query based on timestamp.
  @Override
  public synchronized JsonElement readRca(String rca) {
    JsonElement json;
    if (SQLiteQueryUtils.isTemperatureProfileRca(rca)) {
      json = getTemperatureRca(rca);
    } else {
      json = getNonTemperatureRcas(rca);
    }
    return json;
  }

  private synchronized JsonElement readTemperatureProfileRca(String rca) {
    RcaResponse response = null;
    Field<Integer> primaryKeyField = DSL.field(
        SQLiteQueryUtils.getPrimaryKeyColumnName(ResourceFlowUnit.RCA_TABLE_NAME), Integer.class);
    SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils.buildRcaQuery(create, rca);
    try {
      List<Record> recordList = rcaQuery.fetch();
      if (recordList == null || recordList.isEmpty()) {
        return new JsonObject();
      }
      Record mostRecentRecord = recordList.get(0);
      response = RcaResponse.buildResponse(mostRecentRecord);

      // ClusterTemperatureRca can only be retrieved from the elected master. If the request is
      // made from a data node, it returns a 400 saying it can only be queried from the elected
      // master.
      if (rca.equals(ClusterTemperatureRca.TABLE_NAME)) {
        Field<Integer> foreignKeyField = DSL.field(
            SQLiteQueryUtils.getPrimaryKeyColumnName(ResourceFlowUnit.RCA_TABLE_NAME),
            Integer.class);
        SelectJoinStep<Record> query = SQLiteQueryUtils
            .buildSummaryQuery(create, ClusterTemperatureSummary.TABLE_NAME,
                mostRecentRecord.get(primaryKeyField),
                foreignKeyField);
        Result<Record> temperatureSummary = query.fetch();
        GenericSummary summary =
            ClusterTemperatureSummary.buildSummaryFromDatabase(temperatureSummary, create);
        response.addNestedSummaryList(summary);
      } else if (rca.equalsIgnoreCase(NodeTemperatureRca.TABLE_NAME)) {
        SelectJoinStep<Record> query = SQLiteQueryUtils.buildSummaryQuery(
            create,
            CompactNodeSummary.TABLE_NAME,
            mostRecentRecord.get(primaryKeyField),
            primaryKeyField);
        Result<Record> nodeTemperatureCompactSummary = query.fetch();
        GenericSummary summary =
            CompactNodeSummary.buildSummaryFromDatabase(
                nodeTemperatureCompactSummary, create);
        response.addNestedSummaryList(summary);
      } else {
        // This gives you the full temperature profile for this node.
        SelectJoinStep<Record> query = SQLiteQueryUtils.buildSummaryQuery(
            create,
            NodeLevelDimensionalSummary.SUMMARY_TABLE_NAME,
            mostRecentRecord.get(primaryKeyField),
            primaryKeyField);
        Result<Record> result = query.fetch();
        GenericSummary nodeLevelDimSummary =
            NodeLevelDimensionalSummary.buildFromDb(result.get(0), create);
        response.addNestedSummaryList(nodeLevelDimSummary);
      }
    } catch (DataAccessException dex) {
      if (dex.getMessage().contains("no such table")) {
        JsonObject json = new JsonObject();
        json.addProperty("error", "RCAs are not created yet.");
        return json;
      } else {
        LOG.error("Failed to read temperature profile RCA for {}.", rca, dex);
      }
    }
    return response.toJson();
  }
}
