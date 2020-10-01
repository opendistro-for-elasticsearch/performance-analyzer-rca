/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FileRotate;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * PersistableSlidingWindow is a SlidingWindow which can have its data written to and read from disk
 *
 * <p>The sliding window retains data for slidingWindowSizeInSeconds and aggregates values into time
 * buckets of width bucketSizeInSeconds. For example if slidingWindowSizeInSeconds is 86400 (1 day)
 * and bucketSizeInSeconds is 3600 (1 hr) then there will be at most 25 data points in the sliding window
 * at any given time. It's 25 and not 24 because the oldest datapoints are removed lazily based on
 * arrival time. This shouldn't impact most consumers of this data structure.
 *
 * <p>The data contained in this sliding window is effectively a time series. Consumers may extend this
 * class and define more complex aggregations or analytics functions.
 */
public class PersistableSlidingWindow extends SlidingWindow<SlidingWindowData> {
  private static final Logger LOG = LogManager.getLogger(PersistableSlidingWindow.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  protected static final String SEPARATOR = "\n";

  protected final double bucketSizeInSeconds;

  private SlidingWindowData aggregatedData;
  private Path pathToFile;

  public PersistableSlidingWindow(int slidingWindowSizeInSeconds,
                                  int bucketSizeInSeconds,
                                  TimeUnit timeUnit,
                                  Path filePath) {
    super(slidingWindowSizeInSeconds, timeUnit);
    this.bucketSizeInSeconds = bucketSizeInSeconds;
    this.pathToFile = filePath;
    try {
      if (Files.exists(pathToFile)) {
        loadFromFile(pathToFile);
      } else {
        Files.createFile(pathToFile);
      }
    } catch (IOException e) {
      LOG.error("Couldn't create file {} to perform young generation tuning", pathToFile, e);
      throw new IllegalArgumentException("Couldn't create or read a file at " + filePath);
    }
  }

  @Override
  public void next(SlidingWindowData slidingWindowData) {
    if (aggregatedData == null) {
      aggregatedData = new SlidingWindowData(slidingWindowData.getTimeStamp(), slidingWindowData.getValue());
    } else {
      aggregatedData.setValue(aggregatedData.getValue() + slidingWindowData.getValue());
    }
    long currTimestamp = slidingWindowData.getTimeStamp();
    long timestampDiff = currTimestamp - aggregatedData.getTimeStamp();
    if (timeUnit.convert(timestampDiff, TimeUnit.SECONDS) > bucketSizeInSeconds) {
      try {
        super.next(aggregatedData);
        aggregatedData = null;
        persist();
      } catch (IOException e) {
        LOG.error("Exception persisting sliding window data", e);
      }
    }
  }

  @Override
  public double readSum() {
    return super.readSum() + aggregatedData.getValue();
  }

  public double readCurrentBucket() {
    return aggregatedData.getValue();
  }

  public void loadFromFile(Path path) throws IOException {
    LineIterator it = FileUtils.lineIterator(path.toFile(), "UTF-8");
    try {
      while (it.hasNext()) {
        String line = it.nextLine();
        SlidingWindowData data = objectMapper.readValue(line, SlidingWindowData.class);
        next(data);
      }
    } finally {
      LineIterator.closeQuietly(it);
    }
  }

  public void persist() throws IOException {
    String tmpFile = pathToFile.toString() + RandomStringUtils.randomAlphanumeric(32);
    Path tmpPath = Paths.get(tmpFile);
    Files.createFile(Paths.get(tmpFile));
    BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile, false));
    for (SlidingWindowData data : windowDeque) {
      writer.write(objectMapper.writeValueAsString(data));
      writer.write(SEPARATOR);
    }
    // write to temporary file
    writer.flush();
    // atomic rotate
    FileRotate.rotateFile(tmpPath, pathToFile);
  }
}
