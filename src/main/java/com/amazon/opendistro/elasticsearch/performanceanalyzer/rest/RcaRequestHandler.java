/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rest;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.RemoteNodeRcaRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.RemoteNodeRcaResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.google.gson.JsonElement;
import io.grpc.stub.StreamObserver;

public class RcaRequestHandler {
  public void serve(RemoteNodeRcaRequest request, StreamObserver<RemoteNodeRcaResponse> responseObserver, Persistable persistable) {
    JsonElement responseJson =
        QueryRcaRequestHandler.getTemperatureProfileRca(persistable, request.getRcaName(RemoteNodeRcaRequest.RCA_NAME_FIELD_NUMBER));
    responseObserver.onNext(RemoteNodeRcaResponse.newBuilder().setTemperatureJson(responseJson.toString()).build());
    responseObserver.onCompleted();
  }
}
