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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.request.RemediationRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemediationController {
  private static final Logger LOG = LogManager.getLogger(RemediationController.class);
  private static final int NUM_OF_RETRY = 3;
  private static RemediationController remediationController = null;
  private final BlockingQueue<RemediationRequest> queue;
  private final Gson gson;

  public static RemediationController instance() {
    if (remediationController == null) {
      synchronized (RemediationController.class) {
        if (remediationController == null) {
          remediationController = new RemediationController();
        }
      }
    }
    return remediationController;
  }

  public BlockingQueue<RemediationRequest> getBlockingQueue() {
    return queue;
  }

  public void run() {
    try {
      while (true) {
        RemediationRequest request = queue.take();
        for (int i = 0; i < NUM_OF_RETRY; i++) {
          JsonObject jsonObject = readRemediationPlugin(request);
          if (jsonObject != null) {
            boolean ret = updateRemediationPlugin(jsonObject, request);
            if (ret) {
              break;
            }
          }
        }
      }
    } catch (InterruptedException ie) {
      LOG.error("Remediation controller thread was interrupted. Reason: {}", ie.getMessage());
    }
  }

  private RemediationController() {
    this.queue = new LinkedBlockingDeque<>(100);
    this.gson = new GsonBuilder().setPrettyPrinting().create();
  }

  private JsonObject readRemediationPlugin(RemediationRequest request) {
    StringBuilder response = new StringBuilder();
    try {
      final URL url = request.getUrl();
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String responseLine = null;
      while ((responseLine = in.readLine()) != null) {
        response.append(responseLine.trim());
      }
      in.close();
    } catch (MalformedURLException | ProtocolException me) {
      LOG.error("Remediation Plugin read API connection error,  error message: {}", me.getMessage());
      return null;
    } catch (IOException ie) {
      LOG.error("Remediation Plugin read API IO exception,  error message: {}", ie.getMessage());
      return null;
    }
    JsonObject json = null;
    try {
      json = gson.fromJson(response.toString(), JsonObject.class);
    } catch (JsonSyntaxException je) {
      LOG.error("fail to parse json string,  error message: {}", je.getMessage());
    }
    return json;
  }

  private boolean updateRemediationPlugin(JsonObject jsonResponse, RemediationRequest request) {
    if (jsonResponse == null) {
      return false;
    }
    if (jsonResponse.get(Const.ERROR) != null) {
      LOG.error("Remediation Plugin returns an error. error message: {}", jsonResponse.toString());
      return false;
    }
    JsonElement requestBody = request.getRequestBody(jsonResponse);
    if (requestBody == null) {
      LOG.error("Response sent by Remediation Plugin contains error. API response : {}", jsonResponse.toString());
      return false;
    }

    StringBuilder response = new StringBuilder();
    try {
      final URL url = request.getUrl();
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/json; utf-8");
      connection.setRequestProperty("Accept", "application/json");
      connection.setDoOutput(true);
      OutputStream os = connection.getOutputStream();
      byte[] input = requestBody.getAsString().getBytes("utf-8");
      os.write(input, 0, input.length);

      BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String responseLine = null;
      while ((responseLine = in.readLine()) != null) {
        response.append(responseLine.trim());
      }
    } catch (MalformedURLException
          | ProtocolException
          | UnsupportedEncodingException e) {
      LOG.error("Remediation Plugin post API connection error,  error message: {}", e.getMessage());
      return false;
    } catch (IOException e) {
      LOG.error("Remediation Plugin post API IO exception,  error message: {}", e.getMessage());
      return false;
    }
    JsonObject json = null;
    try {
      json = gson.fromJson(response.toString(), JsonObject.class);
    } catch (JsonSyntaxException je) {
      LOG.error("fail to parse json string,  error message: {}", je.getMessage());
      return false;
    }
    return request.compareResponse(json);
  }
}
