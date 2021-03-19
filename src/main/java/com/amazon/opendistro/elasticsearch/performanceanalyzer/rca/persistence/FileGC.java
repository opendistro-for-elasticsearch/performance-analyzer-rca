/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** This makes sure that we only keep a fixed number of DB files based on age and count. */
public class FileGC {
  private static final Logger LOG = LogManager.getLogger(FileGC.class);

  private final Path DB_DIR;
  private final String BASE_DB_FILENAME;
  private final TimeUnit TIME_UNIT;
  private final long PERIODICITY;
  private final int FILES_COUNT;

  private final String WILDCARD_CHARACTER = "*";

  protected Deque<File> eligibleForGc;

  FileGC(
      Path db_dir,
      String base_db_filename,
      TimeUnit time_unit,
      long periodicity1,
      int files_count) throws IOException {
    this(
        db_dir, base_db_filename, time_unit, periodicity1, files_count, System.currentTimeMillis());
  }

  FileGC(
      Path db_dir,
      String base_db_filename,
      TimeUnit time_unit,
      long periodicity1,
      int files_count,
      long currentMillis) throws IOException {
    DB_DIR = db_dir;
    BASE_DB_FILENAME = base_db_filename;

    TIME_UNIT = time_unit;
    PERIODICITY = periodicity1;
    FILES_COUNT = files_count;

    List<File> remainingFiles = cleanupAndGetRemaining(currentMillis);

    eligibleForGc = new LinkedList<>(remainingFiles);
  }

  /**
   * When we add a newly rotated file, this method checks if the current count of the garbage files
   * is above the threshold, then it removes the oldest file to keep the count under FILES_COUNT.
   *
   * @param filename The name of the new file to be added.
   */
  void eligibleForGc(String filename) throws IOException {
    File file = Paths.get(DB_DIR.toString(), filename).toFile();
    if (file.exists()) {
      eligibleForGc.addLast(file);

      if (eligibleForGc.size() > FILES_COUNT) {
        File oldestFile = eligibleForGc.removeFirst();
        delete(oldestFile);
      }
    }
  }

  protected List<File> cleanupAndGetRemaining(long currentMillis) throws IOException {
    String[] files = getDbFiles();
    List<File> afterCleaningOldFiles = timeBasedCleanup(files, currentMillis);
    return countBasedCleanup(afterCleaningOldFiles);
  }

  protected String[] getDbFiles() {
    return DB_DIR
        .toFile()
        .list(new WildcardFileFilter(BASE_DB_FILENAME + "." + WILDCARD_CHARACTER));
  }

  protected List<File> timeBasedCleanup(String[] files, final long currentMillis) throws IOException {
    long timeDelta = TimeUnit.MILLISECONDS.convert(PERIODICITY, TIME_UNIT) * (FILES_COUNT + 1);
    long timeLimit = currentMillis - timeDelta;

    List<File> remainingFiles = new ArrayList<>();

    for (String file : files) {
      Path filePath = Paths.get(DB_DIR.toString(), file);
      long lastModified = filePath.toFile().lastModified();

      if (lastModified < timeLimit) {
        delete(filePath.toFile());
      } else {
        remainingFiles.add(filePath.toFile());
      }
    }
    return remainingFiles;
  }

  protected List<File> countBasedCleanup(List<File> files) throws IOException {
    int numToDelete = files.size() - FILES_COUNT;
    Collections.sort(files, LastModifiedFileComparator.LASTMODIFIED_COMPARATOR);

    int deletedSoFar = 0;
    List<File> remainingFiles = new ArrayList<>();

    for (File file : files) {
      if (deletedSoFar < numToDelete) {
        delete(file);
        deletedSoFar += 1;
      } else {
        remainingFiles.add(file);
      }
    }
    return remainingFiles;
  }

  private void delete(File file) throws IOException {
    Path path = Paths.get(file.toURI());
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      LOG.error("Could not delete file: {}. Error: {}", file, e);
      throw e;
    }
  }
}
