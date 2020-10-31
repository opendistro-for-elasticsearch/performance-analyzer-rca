/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.jooq.tools.json.JSONObject;

public class RcaTestHelper {
  public static List<String> getAllLinesFromStatsLog() {
    try {
      String statsLog = getLogFilePath(LogType.StatsLog);
      if (statsLog == null) {
        return Collections.emptyList();
      }
      return Files.readAllLines(Paths.get(statsLog));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Collections.EMPTY_LIST;
  }

  public static List<String> getAllLinesWithMatchingString(String pattern) {
    List<String> matches = new ArrayList<>();
    for (String line: getAllLinesFromStatsLog()) {
      if (line.contains(pattern)) {
        matches.add(line);
      }
    }
    return matches;
  }

  public static List<String> getAllLinesFromLog(LogType logType) {
    try {
      String logFilePath = getLogFilePath(logType);
      if (logFilePath == null) {
        return Collections.emptyList();
      }
      return Files.readAllLines(Paths.get(logFilePath));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Collections.EMPTY_LIST;
  }

  public static List<String> getAllLogLinesWithMatchingString(LogType logType, String pattern) {
    List<String> matches = new ArrayList<>();
    for (String line: getAllLinesFromLog(logType)) {
      if (line.contains(pattern)) {
        matches.add(line);
      }
    }
    return matches;
  }

  public enum LogType {
    PerformanceAnalyzerLog,
    StatsLog
  }

  public static String getLogFilePath(LogType logType) {
    System.out.println(LoggerContext.getContext().getRootLogger().getAppenders());
    org.apache.logging.log4j.core.Logger logger = null;
    if (logType == LogType.StatsLog) {
      logger = LoggerContext.getContext().getLogger("stats_log");
    } else {
      logger = LoggerContext.getContext().getRootLogger();
    }
    FileAppender fileAppender = (FileAppender) logger.getAppenders().get(logType.name());
    return fileAppender == null ? null : fileAppender.getFileName();
  }

  public static void cleanUpLogs() {
    String paLog = getLogFilePath(LogType.PerformanceAnalyzerLog);
    if (paLog != null) {
      truncate(Paths.get(paLog).toFile());
    }
    String statsLog = getLogFilePath(LogType.StatsLog);
    if (statsLog != null) {
      truncate(Paths.get(statsLog).toFile());
    }
  }

  public static void setEvaluationTimeForAllNodes(List<ConnectedComponent> connectedComponents,
                                                  long val) {
    for (ConnectedComponent connectedComponent: connectedComponents) {
      for (Node node: connectedComponent.getAllNodes()) {
        node.setEvaluationIntervalSeconds(val);
      }
    }
  }

  public static AppContext setMyIp(String ip, AllMetrics.NodeRole nodeRole) {
    final String separator = System.lineSeparator();
    JSONObject jtime = new JSONObject();
    jtime.put("current_time", 1566414001749L);

    JSONObject jOverrides = new JSONObject();
    long overridesTimestamp = System.currentTimeMillis();

    JSONObject jNode = new JSONObject();
    jNode.put(AllMetrics.NodeDetailColumns.ID.toString(), "4sqG_APMQuaQwEW17_6zwg");
    jNode.put(AllMetrics.NodeDetailColumns.HOST_ADDRESS.toString(), ip);
    jNode.put(AllMetrics.NodeDetailColumns.ROLE.toString(), nodeRole);
    jNode.put(AllMetrics.NodeDetailColumns.IS_MASTER_NODE,
            nodeRole == AllMetrics.NodeRole.ELECTED_MASTER ? true : false);

    ClusterDetailsEventProcessor eventProcessor = new ClusterDetailsEventProcessor();
    StringBuilder nodeDetails = new StringBuilder();
    nodeDetails.append(jtime);
    nodeDetails.append(separator);
    nodeDetails.append(jOverrides);
    nodeDetails.append(separator);
    nodeDetails.append(overridesTimestamp);
    nodeDetails.append(separator);
    nodeDetails.append(jNode.toString());
    eventProcessor.processEvent(
            new Event("", nodeDetails.toString(), 0));
    AppContext appContext = new AppContext();
    appContext.setClusterDetailsEventProcessor(eventProcessor);
    return appContext;
  }

  public static void truncate(File file) {
    try (FileChannel outChan = new FileOutputStream(file, false).getChannel()) {
      outChan.truncate(0);
    } catch (FileNotFoundException e) {
      System.out.println(file.getName() + " does not exist.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void updateConfFileForMutedComponents(String rcaConfPath,
      List<String> mutedComponents, String componentKey) throws Exception {

    // create the config json Object from rca config file
    Scanner scanner = new Scanner(new FileInputStream(rcaConfPath), StandardCharsets.UTF_8.name());
    String jsonText = scanner.useDelimiter("\\A").next();
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(JsonParser.Feature.ALLOW_COMMENTS);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    JsonNode configObject = mapper.readTree(jsonText);

    // update the `MUTED_RCAS_CONFIG` value in config Object
    ArrayNode array = mapper.valueToTree(mutedComponents);
    ((ObjectNode) configObject).putArray(componentKey).addAll(array);
    mapper.writeValue(new FileOutputStream(rcaConfPath), configObject);
  }

}
