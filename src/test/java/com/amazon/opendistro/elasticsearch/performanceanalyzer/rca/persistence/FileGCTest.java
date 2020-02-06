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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaTestHelper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileGCTest {
  private Path testLocation = null;
  private final String baseFilename = "rca.test.file";

  @BeforeClass
  public static void cleanupLogs() {
    RcaTestHelper.cleanUpLogs();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    cleanupLogs();
    String cwd = System.getProperty("user.dir");
    Path tmpPath = Paths.get(cwd, "src", "test", "resources", "tmp");
    FileUtils.cleanDirectory(tmpPath.toFile());
  }

  @Before
  public void init() throws IOException {
    String cwd = System.getProperty("user.dir");
    testLocation = Paths.get(cwd, "src", "test", "resources", "tmp", "file_rotate");
    Files.createDirectories(testLocation);
    FileUtils.cleanDirectory(testLocation.toFile());
  }

  @After
  public void after() throws IOException {
    FileUtils.cleanDirectory(testLocation.toFile());
  }

  class FileGCTestHelper extends FileGC {
    FileGCTestHelper() throws IOException {
      // Based on these numbers the limit for the time based cleanup is [now - (10 * 3)].
      super(testLocation, baseFilename, TimeUnit.MILLISECONDS, 10, 3);
    }

    @Override
    protected List<File> cleanupAndGetRemaining(long currentMillis) {
      return Collections.EMPTY_LIST;
    }

    public Deque<File> getEligibleForGcList() {
      return eligibleForGc;
    }
  }

  @Test
  public void testTimeBasedFileCleanup() throws IOException {
    long currentTime = System.currentTimeMillis();
    // create files with modified time past the limit.
    long limit = currentTime - 10 * 3;

    // For these set of files, we are trying to set the file modification time to be before the
    // limit so that they will be deleted by the time based cleaner.
    for (int i = 0; i < 5; i++) {
      String name = baseFilename + "." + i;
      Path filePath = Paths.get(testLocation.toString(), name);
      Files.createFile(filePath);
      Assert.assertTrue(filePath.toFile().setLastModified(limit - (i + 1) * 10));
    }
    FileGCTestHelper fileGc = new FileGCTestHelper();
    String[] files = fileGc.getDbFiles();
    List<File> afterTimeBasedCleanup = fileGc.timeBasedCleanup(files, System.currentTimeMillis());

    files = fileGc.getDbFiles();
    Assert.assertEquals("Remaining files: " + files, 0, files.length);

    // we expect all files to be cleaned up.
    Assert.assertEquals(0, afterTimeBasedCleanup.size());
  }

  @Test
  public void testCountBasedCleanup() throws IOException {
    long currtTime = System.currentTimeMillis();
    Set<String> set = new HashSet<>();
    List<String> listByRecency = new ArrayList<>();

    // files that are recent enough that they won't be affected by the time based filter.
    for (int j = 0; j < 10; j++) {
      String name = baseFilename + "." + j + j;
      set.add(name);
      listByRecency.add(name);
      Path filePath = Paths.get(testLocation.toString(), name);
      Files.createFile(filePath);
      long lastMod = currtTime - j;
      Assert.assertTrue(filePath.toFile().setLastModified(lastMod));
    }

    FileGCTestHelper fileGc = new FileGCTestHelper();
    String[] files = fileGc.getDbFiles();

    List<File> filesList =
        Arrays.stream(files)
            .map(f -> Paths.get(testLocation.toString(), f).toFile())
            .collect(Collectors.toList());
    List<File> afterCountBasedCleanup = fileGc.countBasedCleanup(filesList);

    Assert.assertEquals(3, afterCountBasedCleanup.size());

    List<String> expectedFiles = listByRecency.subList(0, 3);
    Collections.reverse(expectedFiles);
    Assert.assertEquals(expectedFiles.size(), afterCountBasedCleanup.size());

    int i = 0;
    for (File file : afterCountBasedCleanup) {
      Assert.assertEquals(expectedFiles.get(i), file.getName());
      i += 1;
    }
  }

  @Test
  public void eligibleForGc() throws IOException {
    long currentTime = System.currentTimeMillis();
    long limit = currentTime - 10 * 3;

    // ALl of these will be deleted because of time based cleanup.
    for (int i = 0; i < 3; i++) {
      String name = baseFilename + "." + i;
      Path filePath = Paths.get(testLocation.toString(), name);
      Files.createFile(filePath);
      Assert.assertTrue(filePath.toFile().setLastModified(limit - (i + 1) * 10));
    }

    long currtTime = System.currentTimeMillis();

    // This file will get deleted because of count based cleaning.
    String name = baseFilename + "." + 222;
    Path filePath = Paths.get(testLocation.toString(), name);
    Files.createFile(filePath);
    Assert.assertTrue(filePath.toFile().setLastModified(currtTime - 5));

    // These files will stay on.
    // <basename>.00 -> most recent file
    // <basename>.22 -> oldest file

    List<String> modifiedTimeBased = new ArrayList<>();
    for (int j = 0; j < 3; j++) {
      name = baseFilename + "." + j + j;
      modifiedTimeBased.add(name);
      filePath = Paths.get(testLocation.toString(), name);
      Files.createFile(filePath);
      long lastMod = currtTime - j;
      Assert.assertTrue(filePath.toFile().setLastModified(lastMod));
    }
    Collections.reverse(modifiedTimeBased);

    System.out.println(System.currentTimeMillis());
    FileGC fileGC = new FileGC(testLocation, baseFilename, TimeUnit.MILLISECONDS, 10, 3, currtTime);

    int i = 0;
    for (File file : fileGC.eligibleForGc) {
      Assert.assertEquals(modifiedTimeBased.get(i), file.getName());
      i += 1;
    }

    name = baseFilename + "." + 100;
    filePath = Paths.get(testLocation.toString(), name);
    Files.createFile(filePath);

    // This should cause the oldest file to be deleted
    fileGC.eligibleForGc(name);
    Assert.assertFalse(
        Paths.get(testLocation.toString(), modifiedTimeBased.get(0)).toFile().exists());

    modifiedTimeBased.add(name);

    i = 1;
    for (File file : fileGC.eligibleForGc) {
      Assert.assertEquals(modifiedTimeBased.get(i), file.getName());
      i += 1;
    }
  }
}
