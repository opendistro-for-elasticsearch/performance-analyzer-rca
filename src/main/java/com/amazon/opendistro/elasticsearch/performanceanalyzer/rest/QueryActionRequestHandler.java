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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.InvalidParameterException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;

/**
 * Request Handler that supports querying the latest action set
 *
 * <p>To get the response for the latest action set suggested via DM Framework
 * curl --url "localhost:9600/_opendistro/_performanceanalyzer/actions" -XGET
 * @<code>
 *     {
 *     "LastSuggestedActionSet": [
 *         {
 *             "actionName": "ModifyQueueCapacity",
 *             "actionable": true,
 *             "coolOffPeriod": 300000,
 *             "muted": false,
 *             "nodeIds": "{oSN5LEstThe25y8NrCGyAg}",
 *             "nodeIps": "{10.212.52.87}",
 *             "summary": {
 *                 "Id": "oSN5LEstThe25y8NrCGyAg",
 *                 "Ip": "10.212.52.87",
 *                 "canUpdate": true,
 *                 "coolOffPeriodInMillis": 300000,
 *                 "currentCapacity": 0,
 *                 "desiredCapacity": 1000,
 *                 "resource": 5
 *             }
 *             "timestamp": 1603671580290
 *         }
 *     ]
 * }
 *
 * </code>
 *     <p/>
 */

public class QueryActionRequestHandler extends MetricsHandler implements HttpHandler {
    public static final String ACTION_SET_JSON_NAME = "LastSuggestedActionSet";
    private static final Logger LOG = LogManager.getLogger(QueryActionRequestHandler.class);
    private Persistable persistable;
    private AppContext appContext;
    JsonParser jsonParser;

    public QueryActionRequestHandler(final AppContext appContext) {
        this.appContext = appContext;
         jsonParser = new JsonParser();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();

        if (requestMethod.equalsIgnoreCase("GET")) {
            LOG.debug("Action Query handler called.");
            exchange.getResponseHeaders().set("Content-Type", "application/json");

            try {
                synchronized (this) {
                    String query = exchange.getRequestURI().getQuery();
                    handleActionRequest(exchange);
                }
            } catch (InvalidParameterException e) {
                LOG.error(
                        (Supplier<?>)
                                () ->
                                        new ParameterizedMessage(
                                                "QueryException {} ExceptionCode: {}.",
                                                e.toString(),
                                                StatExceptionCode.REQUEST_ERROR.toString()),
                        e);
                String response = "{\"error\":\"" + e.getMessage() + "\"}";
                sendResponse(exchange, response, HttpURLConnection.HTTP_BAD_REQUEST);
            } catch (Exception e) {
                LOG.error(
                        (Supplier<?>)
                                () ->
                                        new ParameterizedMessage(
                                                "QueryException {} ExceptionCode: {}.",
                                                e.toString(),
                                                StatExceptionCode.REQUEST_ERROR.toString()),
                        e);
                String response = "{\"error\":\"" + e.toString() + "\"}";
                sendResponse(exchange, response, HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
        } else {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        }
        exchange.close();
    }

    private void handleActionRequest(HttpExchange exchange)
            throws IOException {
        //check if we are querying from elected master
        if (!validNodeRole()) {
            JsonObject errResponse = new JsonObject();
            errResponse.addProperty("error", "Node being queried is not elected master.");
            sendResponse(exchange, errResponse.toString(),
                    HttpURLConnection.HTTP_BAD_REQUEST);
            return;
        }

        String response = getActionData(persistable).toString();
        sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
    }

    private JsonElement getActionData(Persistable persistable) {
        LOG.debug("Action: in getActionData");
        JsonObject result = new JsonObject();
        if (persistable != null) {
            try {
                List<PersistedAction> actionSet = persistable.readAllForMaxField(PersistedAction.class,
                        PersistedAction.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME, Long.class);
                JsonArray response = new JsonArray();
                if (actionSet != null) {
                    for (PersistedAction action : actionSet) {
                        response.add(action.toJson(this.jsonParser));
                    }
                    result.add(ACTION_SET_JSON_NAME, response);
                } else {
                    result.add(ACTION_SET_JSON_NAME, new JsonArray());
                }
            } catch (Exception e) {
                LOG.error("Fail to query DB, message : {}", e.getMessage());
                result.add("error", this.jsonParser.parse("Fail to query db").getAsJsonObject());
            }
        }
        return result;
    }

    public void sendResponse(HttpExchange exchange, String response, int status) throws IOException {
        try {
            OutputStream os = exchange.getResponseBody();
            exchange.sendResponseHeaders(status, response.length());
            os.write(response.getBytes());
        } catch (Exception e) {
            response = e.toString();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, response.length());
        }
    }

    public synchronized void setPersistable(Persistable persistable) {
        this.persistable = persistable;
    }

    // check if we are querying from elected master
    private boolean validNodeRole() {
        return appContext.getMyInstanceDetails().getIsMaster();
    }
}
