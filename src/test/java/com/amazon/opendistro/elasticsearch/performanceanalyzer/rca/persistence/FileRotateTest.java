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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

//TODO
@Ignore
public class FileRotateTest {
  private Path fileToRotate = null;
  private static Path testLocation = null;
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-kk-mm-ss");

  @BeforeClass
  public static void cleanupLogs() {
    RcaTestHelper.cleanUpLogs();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    cleanupLogs();
    if (testLocation != null) {
      FileUtils.cleanDirectory(testLocation.toFile());
    }
  }

  @Before
  public void init() throws IOException {
    String cwd = System.getProperty("user.dir");
    testLocation = Paths.get(cwd, "src", "test", "resources", "tmp", "file_rotate");
    Files.createDirectories(testLocation);
    FileUtils.cleanDirectory(testLocation.toFile());
    fileToRotate = Paths.get(testLocation.toString(), "fileRotate.test");
    Files.deleteIfExists(fileToRotate);
  }

  class TestFileRotate extends FileRotate {
    TestFileRotate(TimeUnit rotation_time_unit, long rotation_period) {
      super(fileToRotate, rotation_time_unit, rotation_period, DATE_FORMAT);
    }

    public void setLastRotated(long value) {
      lastRotatedMillis = value;
    }

    @Override
    public Path rotate(long millis) throws IOException {
      return super.rotate(millis);
    }

    @Override
    public boolean shouldRotate(long currentTimeMillis) {
      return super.shouldRotate(currentTimeMillis);
    }
  }

  @Test
  public void shouldRotate() throws InterruptedException, IOException {
    TestFileRotate fileRotate = new TestFileRotate(TimeUnit.MILLISECONDS, 100);
    Thread.sleep(100);
    Assert.assertTrue(fileRotate.shouldRotate(System.currentTimeMillis()));
    fileRotate.setLastRotated(System.currentTimeMillis());
    Assert.assertFalse(fileRotate.shouldRotate(System.currentTimeMillis()));
  }

  //@Test
  public void rotate() throws IOException {
    TestFileRotate fileRotate = new TestFileRotate(TimeUnit.MILLISECONDS, 100);
    Assert.assertFalse(fileToRotate.toFile().exists());
    Assert.assertNull(fileRotate.rotate(System.currentTimeMillis()));

    // Let's create a file and try rotating it.
    long currentMillis = System.currentTimeMillis();

    Files.createFile(fileToRotate);
    Assert.assertTrue(fileToRotate.toFile().exists());
    fileRotate.rotate(currentMillis);

    String formatNow = DATE_FORMAT.format(currentMillis);
    for (String f : testLocation.toFile().list()) {
      String prefix = fileToRotate.getFileName() + "." + formatNow;
      Assert.assertTrue(
          String.format("expected prefix: '%s', found: '%s'", prefix, f), f.startsWith(prefix));
    }

    long lastRotatedMillis = fileRotate.lastRotatedMillis;
    Assert.assertFalse(fileToRotate.toFile().exists());
    Files.createFile(fileToRotate);
    Assert.assertTrue(fileToRotate.toFile().exists());
    fileRotate.rotate(currentMillis);
    Assert.assertEquals("File should not rotate if the rotation target already exists",
            lastRotatedMillis, fileRotate.lastRotatedMillis);
  }

  @Test
  public void tryRotate() throws IOException {
    TestFileRotate fileRotate = new TestFileRotate(TimeUnit.MILLISECONDS, 100);
    long currentMillis = System.currentTimeMillis();
    fileRotate.setLastRotated(currentMillis - 100);

    Files.createFile(fileToRotate);
    Assert.assertTrue(fileToRotate.toFile().exists());
    fileRotate.tryRotate(currentMillis);

    String formatNow = DATE_FORMAT.format(currentMillis);
    for (String f : testLocation.toFile().list()) {
      String prefix = fileToRotate.getFileName() + "." + formatNow;
      Assert.assertTrue(
              String.format("expected prefix: '%s', found: '%s'", prefix, f), f.startsWith(prefix));
    }
    currentMillis = System.currentTimeMillis();
    fileRotate.setLastRotated(currentMillis);
    Assert.assertEquals(null, fileRotate.tryRotate(currentMillis));

  }
}
