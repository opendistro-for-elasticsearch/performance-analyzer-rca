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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HotShardSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.JooqFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.exception.DataTypeException;
import org.jooq.impl.DSL;


/**
 * HotShardSummary contains information such as the index_name, shard_id, node_id, cpu_utilization, cpu_utilization_threshold
 * io_throughput, io_throughput_threshold, io_sys_callrate, io_sys_callrate_threshold and time_period.
 *
 * <p>The hot shard summary is created by node level and cluster level RCAs running on data nodes and elected master node resp.
 * This object is persisted in SQLite table
 * Table name : HotClusterSummary
 *
 *  <p>schema :
 *  | ID(primary key) | index_name | shard_id | node_id | cpu_utilization | cpu_utilization_threshold
 *  | io_throughput | io_throughput_threshold | io_sys_callrate | io_sys_callrate_threshold| ID in FlowUnit(foreign key)
 */

public class HotShardSummary extends GenericSummary {

  public static final String HOT_SHARD_SUMMARY_TABLE = HotShardSummary.class.getSimpleName();
  private static final Logger LOG = LogManager.getLogger(HotShardSummary.class);
  private final String indexName;
  private final String shardId;
  private final String nodeId;
  private double cpu_utilization;
  private double cpu_utilization_threshold;
  private double io_throughput;
  private double io_throughput_threshold;
  private double io_sys_callrate;
  private double io_sys_callrate_threshold;
  private int timePeriodInSeconds;

  public HotShardSummary(String indexName, String shardId, String nodeId, int timePeriod) {
    super();
    this.indexName = indexName;
    this.shardId = shardId;
    this.nodeId = nodeId;
    this.timePeriodInSeconds = timePeriod;
  }

  public void setcpuUtilization(final double cpu_utilization) {
    this.cpu_utilization = cpu_utilization;
  }

  public void setCpuUtilizationThreshold(final double cpu_utilization_threshold) {
    this.cpu_utilization_threshold = cpu_utilization_threshold;
  }

  public void setIoThroughput(final double io_throughput) {
    this.io_throughput = io_throughput;
  }

  public void setIoThroughputThreshold(final double io_throughput_threshold) {
    this.io_throughput_threshold = io_throughput_threshold;
  }

  public void setIoSysCallrate(final double io_sys_callrate) {
    this.io_sys_callrate = io_sys_callrate;
  }

  public void setIoSysCallrateThreshold(final double io_sys_callrate_threshold) {
    this.io_sys_callrate_threshold = io_sys_callrate_threshold;
  }

  public String getIndexName() {
    return this.indexName;
  }

  public String getShardId() {
    return this.shardId;
  }

  public String getNodeId() {
    return this.nodeId;
  }

  public double getCpuUtilization() {
    return this.cpu_utilization;
  }

  public double getIOThroughput() {
    return this.io_throughput;
  }

  public double getIOSysCallrate() {
    return this.io_sys_callrate;
  }

  @Override
  public HotShardSummaryMessage buildSummaryMessage() {
    final HotShardSummaryMessage.Builder summaryMessageBuilder = HotShardSummaryMessage
            .newBuilder();
    summaryMessageBuilder.setIndexName(this.indexName);
    summaryMessageBuilder.setShardId(this.shardId);
    summaryMessageBuilder.setNodeId(this.nodeId);
    summaryMessageBuilder.setCpuUtilization(this.cpu_utilization);
    summaryMessageBuilder.setCpuUtilizationThreshold(this.cpu_utilization_threshold);
    summaryMessageBuilder.setIoThroughput(this.io_throughput);
    summaryMessageBuilder.setIoThroughputThreshold(this.io_throughput_threshold);
    summaryMessageBuilder.setIoSysCallrate(this.io_sys_callrate);
    summaryMessageBuilder.setIoSysCallrateThreshold(this.io_sys_callrate_threshold);
    summaryMessageBuilder.setTimePeriod(this.timePeriodInSeconds);
    return summaryMessageBuilder.build();
  }

  @Override
  public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
    messageBuilder.setHotShardSummary(this.buildSummaryMessage());
  }

  public static HotShardSummary buildHotShardSummaryFromMessage(HotShardSummaryMessage message) {
    HotShardSummary summary = new HotShardSummary(message.getIndexName(), message.getShardId(),
            message.getNodeId(), message.getTimePeriod());
    summary.setcpuUtilization(message.getCpuUtilization());
    summary.setCpuUtilizationThreshold(message.getCpuUtilizationThreshold());
    summary.setIoThroughput(message.getIoThroughput());
    summary.setIoThroughputThreshold(message.getIoThroughputThreshold());
    summary.setIoSysCallrate(message.getIoSysCallrate());
    summary.setIoSysCallrateThreshold(message.getIoSysCallrateThreshold());
    return summary;
  }

  @Override
  public String toString() {
    return String.join(" ", new String[]
            { this.indexName, this.shardId, this.nodeId,
                    String.valueOf(this.cpu_utilization), String.valueOf(this.cpu_utilization_threshold),
                    String.valueOf(this.io_throughput), String.valueOf(this.io_throughput_threshold),
                    String.valueOf(io_sys_callrate), String.valueOf(io_sys_callrate_threshold)
            });
  }

  @Override
  public String getTableName() {
    return HotShardSummary.HOT_SHARD_SUMMARY_TABLE;
  }

  @Override
  public List<SummaryBuilder<? extends GenericSummary>> getNestedSummaryBuilder() {
    return Collections.unmodifiableList(Collections.singletonList(
            new SummaryBuilder<>(HotShardSummary.HOT_SHARD_SUMMARY_TABLE,
                    HotNodeSummary::buildSummary)));
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    List<Field<?>> schema = new ArrayList<>();
    schema.add(HotShardSummaryField.INDEX_NAME_FIELD.getField());
    schema.add(HotShardSummaryField.SHARD_ID_FIELD.getField());
    schema.add(HotShardSummaryField.NODE_ID_FIELD.getField());
    schema.add(HotShardSummaryField.CPU_UTILIZATION_FIELD.getField());
    schema.add(HotShardSummaryField.CPU_UTILIZATION_THRESHOLD_FIELD.getField());
    schema.add(HotShardSummaryField.IO_THROUGHPUT_FIELD.getField());
    schema.add(HotShardSummaryField.IO_THROUGHPUT_THRESHOLD_FIELD.getField());
    schema.add(HotShardSummaryField.IO_SYSCALLRATE_FIELD.getField());
    schema.add(HotShardSummaryField.IO_SYSCALLRATE_THRESHOLD_FIELD.getField());
    schema.add(HotShardSummaryField.TIME_PERIOD_FIELD.getField());
    return schema;
  }

  @Override
  public List<Object> getSqlValue() {
    List<Object> value = new ArrayList<>();
    value.add(this.indexName);
    value.add(this.shardId);
    value.add(this.nodeId);
    value.add(this.cpu_utilization);
    value.add(this.cpu_utilization_threshold);
    value.add(this.io_throughput);
    value.add(this.io_throughput_threshold);
    value.add(this.io_sys_callrate);
    value.add(this.io_sys_callrate_threshold);
    value.add(Integer.valueOf(this.timePeriodInSeconds));
    return value;
  }

  /**
   * Convert this summary object to JsonElement
   * @return JsonElement
   */
  @Override
  public JsonElement toJson() {
    JsonObject summaryObj = new JsonObject();
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.INDEX_NAME_COL_NAME, this.indexName);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.SHARD_ID_COL_NAME, this.shardId);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME, this.nodeId);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.CPU_UTILIZATION_COL_NAME, this.cpu_utilization);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.CPU_UTILIZATION_THRESHOLD_COL_NAME, this.cpu_utilization_threshold);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.IO_THROUGHPUT_COL_NAME, this.io_throughput);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.IO_THROUGHPUT_THRESHOLD_COL_NAME, this.io_throughput_threshold);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.IO_SYSCALLRATE_COL_NAME, this.io_sys_callrate);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.IO_SYSCALLRATE_THRESHOLD_COL_NAME, this.io_sys_callrate_threshold);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME, this.timePeriodInSeconds);
    return summaryObj;
  }

  public static class SQL_SCHEMA_CONSTANTS {
    public static final String INDEX_NAME_COL_NAME = "index_name";
    public static final String SHARD_ID_COL_NAME = "shard_id";
    public static final String NODE_ID_COL_NAME = "node_id";
    public static final String CPU_UTILIZATION_COL_NAME = "cpu_utilization";
    public static final String CPU_UTILIZATION_THRESHOLD_COL_NAME = "cpu_utilization_threshold";
    public static final String IO_THROUGHPUT_COL_NAME = "io_throughput";
    public static final String IO_THROUGHPUT_THRESHOLD_COL_NAME = "io_throughput_threshold";
    public static final String IO_SYSCALLRATE_COL_NAME = "io_sys_callrate";
    public static final String IO_SYSCALLRATE_THRESHOLD_COL_NAME = "io_sys_callrate_threshold";
    public static final String TIME_PERIOD_COL_NAME = "time_period";

  }

  /**
   * Cluster summary SQL fields
   */
  public enum HotShardSummaryField implements JooqFieldValue {
    INDEX_NAME_FIELD(SQL_SCHEMA_CONSTANTS.INDEX_NAME_COL_NAME, String.class),
    SHARD_ID_FIELD(SQL_SCHEMA_CONSTANTS.SHARD_ID_COL_NAME, String.class),
    NODE_ID_FIELD(SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME, String.class),
    CPU_UTILIZATION_FIELD(SQL_SCHEMA_CONSTANTS.CPU_UTILIZATION_COL_NAME, Double.class),
    CPU_UTILIZATION_THRESHOLD_FIELD(SQL_SCHEMA_CONSTANTS.CPU_UTILIZATION_THRESHOLD_COL_NAME, Double.class),
    IO_THROUGHPUT_FIELD(SQL_SCHEMA_CONSTANTS.IO_THROUGHPUT_COL_NAME, Double.class),
    IO_THROUGHPUT_THRESHOLD_FIELD(SQL_SCHEMA_CONSTANTS.IO_THROUGHPUT_THRESHOLD_COL_NAME, Double.class),
    IO_SYSCALLRATE_FIELD(SQL_SCHEMA_CONSTANTS.IO_SYSCALLRATE_COL_NAME, Double.class),
    IO_SYSCALLRATE_THRESHOLD_FIELD(SQL_SCHEMA_CONSTANTS.IO_SYSCALLRATE_THRESHOLD_COL_NAME, Double.class),
    TIME_PERIOD_FIELD(SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME, Integer.class);

    private String name;
    private Class<?> clazz;

    HotShardSummaryField(final String name, Class<?> clazz) {
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

  /**
   * Re-generate the node summary object from SQL query result.
   * @param record SQLite record
   * @return node summary object
   */
  @Nullable
  public static HotShardSummary buildSummary(final Record record) {
    HotShardSummary summary = null;
    try {
      String indexName = record.get(HotShardSummaryField.INDEX_NAME_FIELD.getField(), String.class);
      String shardId = record.get(HotShardSummaryField.SHARD_ID_FIELD.getField(), String.class);
      String nodeId = record.get(HotShardSummaryField.NODE_ID_FIELD.getField(), String.class);
      Double cpu_utilization = record.get(HotShardSummaryField.CPU_UTILIZATION_FIELD.getField(), Double.class);
      Double cpu_utilization_threshold = record.get(HotShardSummaryField.CPU_UTILIZATION_THRESHOLD_FIELD.getField(), Double.class);
      Double io_throughput = record.get(HotShardSummaryField.IO_THROUGHPUT_FIELD.getField(), Double.class);
      Double io_throughput_threshold = record.get(HotShardSummaryField.IO_THROUGHPUT_THRESHOLD_FIELD.getField(), Double.class);
      Double io_sys_callrate = record.get(HotShardSummaryField.IO_SYSCALLRATE_FIELD.getField(), Double.class);
      Double io_sys_callrate_threshold = record.get(HotShardSummaryField.IO_SYSCALLRATE_THRESHOLD_FIELD.getField(), Double.class);
      Integer timePeriod = record.get(HotShardSummaryField.TIME_PERIOD_FIELD.getField(), Integer.class);
      summary = new HotShardSummary(indexName, shardId, nodeId, timePeriod);
      summary.setcpuUtilization(cpu_utilization);
      summary.setCpuUtilizationThreshold(cpu_utilization_threshold);
      summary.setIoThroughput(io_throughput);
      summary.setIoThroughputThreshold(io_throughput_threshold);
      summary.setIoSysCallrate(io_sys_callrate);
      summary.setIoSysCallrateThreshold(io_sys_callrate_threshold);
    }
    catch (IllegalArgumentException ie) {
      LOG.error("Some fields might not be found in record, cause : {}", ie.getMessage());
    }
    catch (DataTypeException de) {
      LOG.error("Fails to convert data type");
    }
    // we are very unlikely to catch this exception unless some fields are not persisted properly.
    catch (NullPointerException ne) {
      LOG.error("read null object from SQL, trace : {} ", ne.getStackTrace());
    }
    return summary;
  }
}
