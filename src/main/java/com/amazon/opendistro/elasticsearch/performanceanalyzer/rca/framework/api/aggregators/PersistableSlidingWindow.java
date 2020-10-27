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
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * PersistableSlidingWindow is a SlidingWindow which can have its data written to and read from disk
 */
public class PersistableSlidingWindow extends SlidingWindow<SlidingWindowData> {
  private static final Logger LOG = LogManager.getLogger(PersistableSlidingWindow.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  // The time to wait between writes, currently 5 minutes TODO (make this configurable)
  private static final long WRITE_PERIOD_MS = 300000L;

  private Path pathToFile;
  private boolean enablePersistence;
  private long lastWriteTimeEpochMs;

  public PersistableSlidingWindow(int slidingWindowSize,
                                  TimeUnit timeUnit,
                                  Path filePath) {
    super(slidingWindowSize, timeUnit);
    this.pathToFile = filePath;
    this.enablePersistence = this.pathToFile != null;
    if (!enablePersistence) {
      LOG.debug("Persistence is not enabled for {}:{}", this.getClass().getSimpleName(), this);
      return;
    }
    this.lastWriteTimeEpochMs = Instant.now().toEpochMilli();
    if (Files.exists(filePath)) {
      try {
        load(this.pathToFile);
      } catch (IOException ex) {
        LOG.error("Unable to load previous data from {} into {}", this.pathToFile,
            getClass().getSimpleName(), ex);
      }
    } else {
      LOG.warn("{}:{} attempted to load data from {}, but the file doesn't exist",
          this.getClass().getSimpleName(), this, filePath);
    }
  }

  /**
   * Loads the SlidingWindowData contained in the given path into this PersistableSlidingWindow
   * @param path The path to the file containing the SlidingWindow data
   * @throws IOException If there is an error reading the file
   */
  protected synchronized void load(Path path) throws IOException {
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

  /**
   * Writes the contents of this SlidingWindow into the path provided during construction
   *
   * <p>This write occurs in 2 stages, it writes to a temporary file, then replaces the actual data
   * file with the contents of the temporary file
   *
   * @throws IOException If there is a CRUD error with the files involved
   */
  protected synchronized void write() throws IOException {
    // only write if persistence is enabled and once every write period
    if (!enablePersistence || WRITE_PERIOD_MS < Instant.now().toEpochMilli() - lastWriteTimeEpochMs) {
      return;
    }
    String tmpFile = pathToFile.toString();
    Path tmpPath = Paths.get(tmpFile);
    if (!Files.exists(tmpPath)) {
      Files.createFile(tmpPath);
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile, false))) {
      Iterator<SlidingWindowData> it = windowDeque.descendingIterator();
      while (it.hasNext()) {
        writer.write(objectMapper.writeValueAsString(it.next()));
        writer.write(System.lineSeparator());
      }
      // write to temporary file
      writer.flush();
      // atomic rotate
      FileRotate.rotateFile(tmpPath, pathToFile);
      lastWriteTimeEpochMs = Instant.now().toEpochMilli();
    }
  }
}
