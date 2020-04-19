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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataTypeException;

public class SQLParsingUtil {
  private static final Logger LOG = LogManager.getLogger(SQLParsingUtil.class);

  /**
   * retrieve the record from result list if the data stored in one particular field
   * of that record matches the name string.
   * @param result the record result from SQL query
   * @param field the field used for name matching
   * @param name the name to be matched
   * @return The record that contain the name string in field
   * @throws IllegalArgumentException throws an exception if we can't find the name in the field
   */
  private static Record getRecordByName(Result<Record> result, Field<String> field, String name) throws IllegalArgumentException {
    int idx = -1;
    if (result != null) {
      List<String> fieldNames = result.getValues(field);
      for (int i = 0; i < fieldNames.size(); i++) {
        if (fieldNames.get(i).equals(name)) {
          idx = i;
          break;
        }
      }
    }
    if (idx < 0) {
      throw new IllegalArgumentException();
    }
    return result.get(idx);
  }

  /**
   * Read data from the SQL result queried from metricDB
   * metric table will look something similar to :
   * [[MemType, sum, avg, min, max],
   * [Eden, 8.6555376E7, 8.6555376E7, 8.6555376E7, 8.6555376E7],
   * [Heap, 5.4237588E8, 5.4237588E8, 5.4237588E8, 5.4237588E8],
   * For example :
   * matchedField => "MemType", matchedFieldName => "Heap", dataField => "Max"
   * and we will read 5.4237588E8 from the result
   * @param result the record result from SQL query
   * @param matchedField the field used for name matching
   * @param matchedFieldName the name to be matched
   * @param dataField The field to retrieve data from
   * @return the data read from SQL result
   */
  public static double readDataFromSqlResult(Result<Record> result, Field<String> matchedField, String matchedFieldName, String dataField) {
    double ret = Double.NaN;
    if (result != null) {
      try {
        Record record = SQLParsingUtil.getRecordByName(result, matchedField, matchedFieldName);
        ret = record.getValue(dataField, Double.class);
      }
      catch (IllegalArgumentException ie) {
        LOG.error("{} fails to match any row in field {}.", matchedFieldName, matchedField.getName());
      }
      catch (
          DataTypeException de) {
        LOG.error("Fail to read {} field from SQL result", dataField);
      }
    }
    return ret;
  }
}
