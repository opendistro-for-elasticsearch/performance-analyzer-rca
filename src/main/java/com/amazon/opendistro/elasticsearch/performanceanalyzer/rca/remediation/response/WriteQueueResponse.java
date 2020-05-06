package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.response;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.Const;
import com.google.gson.JsonObject;
import javax.annotation.Nullable;

public class WriteQueueResponse extends RemediationResponse {
  private int capacity;

  public WriteQueueResponse(int capacity) {
    this.capacity = capacity;
  }

  public int getCapacity() {
    return capacity;
  }

  @Nullable
  public static WriteQueueResponse build(final JsonObject object) {
    JsonObject statObj = object.getAsJsonObject(Const.QUEUE_STATS);
    if (statObj == null) {
      return null;
    }
    JsonObject queueObj = statObj.getAsJsonObject(Const.WRITE_QUEUE);
    if (queueObj == null) {
      return null;
    }
    JsonObject capObj = queueObj.getAsJsonObject(Const.CAPACITY);
    if (capObj == null) {
      return null;
    }
    return new WriteQueueResponse(capObj.getAsInt());
  }
}
