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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit.ResourceFlowUnitFieldValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit.SQL_SCHEMA_CONSTANTS;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.SummaryBuilder;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.exception.DataTypeException;

/**
 * RcaResponse contains cluster level info such as cluster state, number of healthy and unhealthy
 * nodes for a particular rca.
 */
public class RcaResponse extends GenericSummary {
  private static final Logger LOG = LogManager.getLogger(RcaResponse.class);
  private String rcaName;
  private String state;
  private long timeStamp;

  public RcaResponse(String rcaName, String state, long timeStamp) {
    this.rcaName = rcaName;
    this.state = state;
    this.timeStamp = timeStamp;
  }


  public String getRcaName() {
    return rcaName;
  }

  public String getState() {
    return state;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public static RcaResponse buildResponse(Record record) {
    RcaResponse response = null;
    try {
      String rcaName = record.get(ResourceFlowUnitFieldValue.RCA_NAME_FILELD.getField(), String.class);
      String state = record.get(ResourceFlowUnitFieldValue.STATE_NAME_FILELD.getField(), String.class);
      Long timeStamp = record.get(ResourceFlowUnitFieldValue.TIMESTAMP_FIELD.getField(), Long.class);
      response = new RcaResponse(rcaName, state, timeStamp);
    }
    catch (IllegalArgumentException ie) {
      LOG.error("Some field is not found in record, cause : {}", ie.getMessage());
    }
    catch (DataTypeException de) {
      LOG.error("Fails to convert data type");
    }
    // we are very unlikely to catch this exception unless some fields are not persisted properly.
    catch (NullPointerException ne) {
      LOG.error("read null object from SQL, trace : {} ", ne.getStackTrace());
    }
    return response;
  }

  /**
   * Since RcaResponse Object is a API wrapper for Flowunit & summaries
   * we do not need to support gPRC. Neither will we persist this wrapper.
   */
  @Override
  public GeneratedMessageV3 buildSummaryMessage() {
    return null;
  }

  @Override
  public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
  }

  @Override
  public String getTableName() {
    return ResourceFlowUnit.RCA_TABLE_NAME;
  }

  @Override
  public List<SummaryBuilder<? extends GenericSummary>> getNestedSummaryBuilder() {
    return Collections.unmodifiableList(Collections.singletonList(
        new SummaryBuilder<>(HotClusterSummary.HOT_CLUSTER_SUMMARY_TABLE,
            HotClusterSummary::buildSummary)));
  }

  @Override
  public List<Field<?>> getSqlSchema() {
    return null;
  }

  @Override
  public List<Object> getSqlValue() {
    return null;
  }

  @Override
  public JsonElement toJson() {
    JsonObject summaryObj = new JsonObject();
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.RCA_COL_NAME, this.rcaName);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME, this.timeStamp);
    summaryObj.addProperty(SQL_SCHEMA_CONSTANTS.STATE_COL_NAME, this.state);
    if (!getNestedSummaryList().isEmpty()) {
      String tableName = getNestedSummaryList().get(0).getTableName();
      summaryObj.add(tableName, this.nestedSummaryListToJson());
    }
    return summaryObj;
  }
}
