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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileRotate {
  private static final Logger LOG = LogManager.getLogger(FileRotate.class);

  private final Path FILE_TO_ROTATE;
  private final String FILENAME;
  private final TimeUnit ROTATION_TIME_UNIT;
  private final long ROTATION_PERIOD;
  private final DateFormat ROTATED_FILE_FORMAT;
  protected long lastRotatedMillis;
  private static final String FILE_PART_SEPARATOR = ".";

  FileRotate(
      Path file_to_rotate,
      TimeUnit rotation_time_unit,
      long rotation_period,
      DateFormat rotated_file_format) {
    FILE_TO_ROTATE = file_to_rotate;
    FILENAME = file_to_rotate.toFile().getName();
    ROTATION_TIME_UNIT = rotation_time_unit;
    ROTATION_PERIOD = rotation_period;
    ROTATED_FILE_FORMAT = rotated_file_format;
    lastRotatedMillis = System.currentTimeMillis();
  }

  /**
   * Try to rotate the file.
   *
   * <p>The file is rotated only if the current time is past the ROTATION_PERIOD.
   *
   * @return null if the file was not rotated because it is not old enough, or the name of the file
   *     after rotation.
   */
  synchronized Path tryRotate(long currentTimeMillis) throws IOException {
    if (shouldRotate(currentTimeMillis)) {
      return rotate(currentTimeMillis);
    }
    return null;
  }

  /**
   * This does not check for the validity of the condition for rotation. Just rotates the file.
   *
   * <p>This can be called when say, the DB file is corrupted and we just want to rotate and create
   * a new file instead of throwing away data in this iteration.
   *
   * @return Path to the file after it is rotated or null if it ran into a problem trying to do so.
   */
  synchronized Path forceRotate(long currentTimeMillis) throws IOException {
    return rotate(currentTimeMillis);
  }

  /**
   * This checks for the condition for file rotation.
   *
   * <p>This usually checks if the time since last rotation is past the {@code ROTATION_PERIOD}.
   *
   * @return
   */
  protected boolean shouldRotate(long currentTimeMillis) {
    long timeSinceLastRotation = currentTimeMillis - lastRotatedMillis;
    long timeUnitsPassed = ROTATION_TIME_UNIT.convert(timeSinceLastRotation, TimeUnit.MILLISECONDS);
    return timeUnitsPassed >= ROTATION_PERIOD;
  }

  private void tryDelete(Path file) throws IOException {
    try {
      if (!Files.deleteIfExists(file)) {
        LOG.warn("The file to delete didn't exist: {}", file);
      }
    } catch (IOException ioException) {
      LOG.error(ioException);
      throw ioException;
    }
  }

  /**
   * Rotate the file.
   *
   * <p>Rotating a file renames it to filename.#current time in the date format specified#. For
   * cases the file could not be renamed, we attempt to delete the file. If the file could not be
   * deleted either, we throw an IOException for the caller to handle or take the necessary action.
   *
   * @return Returns the path to the file after it was rotated, this is so that the GC can add it to the list.
   *
   * @throws IOException when it can't even delete the current file.
   */
  protected synchronized Path rotate(long currentMillis) throws IOException {
    if (!FILE_TO_ROTATE.toFile().exists()) {
      return null;
    }
    if (FILE_TO_ROTATE.getParent() == null) {
      return null;
    }

    String dir = FILE_TO_ROTATE.getParent().toString();
    StringBuilder targetFileName = new StringBuilder(FILENAME);
    targetFileName.append(FILE_PART_SEPARATOR).append(ROTATED_FILE_FORMAT.format(currentMillis));

    Path targetFilePath = Paths.get(dir, targetFileName.toString());

    Path ret;

    // Fallback in rotating a file:
    // try 1. Rotate the file, don't try to replace the destination file if one exists.
    // try 2: Rotate the file now with replacement and add a log saying the destination file will be deleted.
    // try 3: Delete the file, don't rotate. The caller will create a new file and start over.
    // try 4: If the delete fails, all bets are off, throw an exception and let the caller decide.
    try {
      ret = Files.move(FILE_TO_ROTATE, targetFilePath, StandardCopyOption.ATOMIC_MOVE);
      lastRotatedMillis = System.currentTimeMillis();
    } catch (FileAlreadyExistsException fae) {
      LOG.error("Deleting file '{}' or else we cannot rotate the current {}", targetFilePath, FILE_TO_ROTATE);
      if (!Files.deleteIfExists(targetFilePath)) {
        LOG.error("Could not delete file: " + targetFilePath);
      }
      try {
        ret = Files.move(FILE_TO_ROTATE, targetFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        lastRotatedMillis = System.currentTimeMillis();
      } catch (Exception ex) {
        LOG.error(ex);
        LOG.error("Deleting file: {}", FILE_TO_ROTATE);
        tryDelete(FILE_TO_ROTATE);

        // Because we are deleting the current file, there is nothing for the GC to add.
        ret = null;
      }
    } catch (IOException e) {
      LOG.error("Could not RENAME file '{}' to '{}'. Error: {}", FILE_TO_ROTATE, targetFilePath, e);
      tryDelete(FILE_TO_ROTATE);

      // Because we are deleting the current file, there is nothing for the GC to add.
      ret = null;
    }

    // If we are here then we have successfully rotated or deleted the FILE_TO_ROTATE.
    // In both the cases a new file will be created by the caller and that file should exist
    // for the ROTATION_PERIOD.
    lastRotatedMillis = System.currentTimeMillis();
    return ret;
  }
}
