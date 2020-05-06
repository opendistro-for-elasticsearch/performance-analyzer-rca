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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.request;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.Const;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.response.WriteQueueResponse;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WriteQueueRequest extends RemediationRequest {
  private static final Logger LOG = LogManager.getLogger(WriteQueueRequest.class);
  private static final String SIFI_THREADPOOL_WRITE_URL = "http://localhost:9200/_sifi/threadpool/write";
  private final Action action;
  private JsonObject requestBody;
  //TODO: for test only. those needs to be moved into Remediation Plugin and
  // expose an API for customer to override this.
  private final int upperBound;
  private final int lowerBound;

  public WriteQueueRequest(final Action action) {
    super();
    this.action = action;
    upperBound = 300;
    lowerBound = 20;
    requestBody = null;
  }

  @Override
  public URL getUrl() throws MalformedURLException {
    return new URL(SIFI_THREADPOOL_WRITE_URL);
  }

  @Override
  public JsonElement getRequestBody(JsonObject response) {
    WriteQueueResponse responseObj = WriteQueueResponse.build(response);
    if (responseObj == null) {
      return null;
    }
    int currCapacity = responseObj.getCapacity();
    if (action == Action.SIZE_DOWN_TO_MIN) {
      currCapacity = lowerBound;
    }
    else {
      currCapacity += action.value;
      currCapacity = Math.max(lowerBound, currCapacity);
      currCapacity = Math.min(upperBound, currCapacity);
    }
    JsonObject requestBodyObj = new JsonObject();
    requestBodyObj.addProperty(Const.MAX_CAPACITY, currCapacity);
    requestBody = requestBodyObj;
    return requestBodyObj;
  }

  @Override
  public boolean compareResponse(JsonObject response) {
    if (requestBody == null) {
      return false;
    }
    JsonObject obj = requestBody.getAsJsonObject(Const.MAX_CAPACITY);
    if (obj == null) {
      return false;
    }
    WriteQueueResponse responseObj = WriteQueueResponse.build(response);
    if (responseObj == null) {
      return false;
    }
    return obj.getAsInt() == responseObj.getCapacity();
  }

  public enum Action {
    SIZE_DOWN_TO_MIN(null),
    SIZE_UP_BY_10(10),
    SIZE_DOWN_BY_10(-10);

    private final Integer value;

    Action(final Integer value) {
      this.value = value;
    }
  }
}
