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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class RcaTestHelper {
  public static List<String> getAllLinesFromStatsLog() {
    try {
      return Files.readAllLines(Paths.get(getLogFilePath("StatsLog")));
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
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

  public static List<String> getAllLinesFromLog(String logName) {
    try {
      return Files.readAllLines(Paths.get(getLogFilePath(logName)));
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      e.printStackTrace();
    }
    return Collections.EMPTY_LIST;
  }

  public static List<String> getAllLogLinesWithMatchingString(String logName, String pattern) {
    List<String> matches = new ArrayList<>();
    for (String line: getAllLinesFromLog(logName)) {
      if (line.contains(pattern)) {
        matches.add(line);
      }
    }
    return matches;
  }

  public static String getLogFilePath(String filename)
      throws ParserConfigurationException, IOException, SAXException, XPathExpressionException {
    String cwd = System.getProperty("user.dir");
    String testResourcesPath =
        Paths.get(Paths.get(cwd, "src", "test", "resources").toString(), "log4j2.xml").toString();

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(testResourcesPath);
    XPathFactory xPathfactory = XPathFactory.newInstance();
    XPath xpath = xPathfactory.newXPath();
    return xpath.evaluate(
        String.format("Configuration/Appenders/File[@name='%s']/@fileName", filename), doc);
  }

  public static void cleanUpLogs() {
    try {
      truncate(Paths.get(getLogFilePath("PerformanceAnalyzerLog")).toFile());
      truncate(Paths.get(getLogFilePath("StatsLog")).toFile());
    } catch (ParserConfigurationException | SAXException | XPathExpressionException | IOException e) {
      e.printStackTrace();
    }
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
}
