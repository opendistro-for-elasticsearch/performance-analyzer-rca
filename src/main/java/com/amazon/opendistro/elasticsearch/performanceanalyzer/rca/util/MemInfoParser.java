/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MemInfoParser {

  private static final Logger LOG = LogManager.getLogger(MemInfoParser.class);
  private static final String MEM_INFO_PATH = "/proc/meminfo";
  private static final String MEM_TOTAL_PREFIX = "MemTotal:";
  private static final long KB_TO_B = 1024L;

  public static long getTotalMemory() {
    try {
      List<String> lines = Files.readAllLines(Paths.get(MEM_INFO_PATH));
      for (String line : lines) {
        if (line.startsWith(MEM_TOTAL_PREFIX)) {
          return extractTotalMemory(line);
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to read total memory", e);
      StatsCollector.instance().logException(StatExceptionCode.TOTAL_MEM_READ_ERROR);
    }

    return -1;
  }

  private static long extractTotalMemory(final String memLine) {
    String[] parsedLine = memLine.trim().replaceAll("\\s+", " ").split(" ");
    if (parsedLine.length != 3) {
      return -1;
    }

    try {
      return Long.parseLong(parsedLine[1]) * KB_TO_B;
    } catch (NumberFormatException numberFormatException) {
      LOG.error("Unable to parse memInfoLine: " + memLine, numberFormatException);
      StatsCollector.instance().logException(StatExceptionCode.TOTAL_MEM_READ_ERROR);
    }

    return -1;
  }
}
