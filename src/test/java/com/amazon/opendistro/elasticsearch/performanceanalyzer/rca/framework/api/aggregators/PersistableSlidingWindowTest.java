/*
 *  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PersistableSlidingWindowTest {
  private static final Logger LOG = LogManager.getLogger(PersistableSlidingWindowTest.class);

  private static Path testLocation;
  private static Path persistFile;

  @Before
  public void init() throws IOException {
    String cwd = System.getProperty("user.dir");
    testLocation = Paths.get(cwd, "src", "test", "resources", "tmp", "PersistableSlidingWindowTest");
    Files.createDirectories(testLocation);
    FileUtils.cleanDirectory(testLocation.toFile());
    persistFile = Paths.get(testLocation.toString(), "PersistableSlidingWindowTest.test");
    Files.deleteIfExists(persistFile);
  }

  @Test
  public void testNext() throws Exception {
    PersistableSlidingWindow slidingWindow = new PersistableSlidingWindow(5, TimeUnit.SECONDS, persistFile);
    long curTimestamp = Instant.now().toEpochMilli();
    // Load data into the window
    slidingWindow.next(new SlidingWindowData(curTimestamp, 10));
    slidingWindow.next(new SlidingWindowData(curTimestamp, 2));
    slidingWindow.next(new SlidingWindowData(curTimestamp, 12));
    slidingWindow.next(new SlidingWindowData(curTimestamp, 18));
    // NOTE even though 5900ms is 5.9s, SlidingWindowData timestamps are truncated, so this point
    // will NOT kick out the other datapoints
    slidingWindow.next(new SlidingWindowData(curTimestamp + 5900, 19));
    Assert.assertEquals(5, slidingWindow.size());
    // Now the older datapoints will get kicked out
    slidingWindow.next(new SlidingWindowData(curTimestamp + 6100, 66));
    slidingWindow.next(new SlidingWindowData(curTimestamp + 6700, 1));
    Assert.assertEquals(3, slidingWindow.size());
    // Test that we can read our writes
    slidingWindow.write();
    PersistableSlidingWindow slidingWindow2 = new PersistableSlidingWindow(5, TimeUnit.SECONDS, persistFile);
    Assert.assertTrue(SlidingWindowTestUtil.equals(slidingWindow, slidingWindow2));
    Assert.assertTrue(SlidingWindowTestUtil.equals_MUTATE(slidingWindow, slidingWindow2));
  }

  @Test
  public void testDifferentTimeUnits() throws Exception {
    PersistableSlidingWindow slidingWindow = new PersistableSlidingWindow(5000000, TimeUnit.MICROSECONDS, persistFile);
    long curTimestamp = Instant.now().toEpochMilli();
    // Load data into the window
    slidingWindow.next(new SlidingWindowData(curTimestamp, 10));
    slidingWindow.next(new SlidingWindowData(curTimestamp, 2));
    slidingWindow.next(new SlidingWindowData(curTimestamp, 12));
    slidingWindow.next(new SlidingWindowData(curTimestamp, 18));
    Assert.assertEquals(4, slidingWindow.size());
    // Test that bucket writes occur properly
    slidingWindow.next(new SlidingWindowData(curTimestamp + 6100, 66));
    Assert.assertEquals(1, slidingWindow.size());
  }

  /**
   * Tests that a PSW can call write() multiple times and still have its data read
   */
  @Test
  public void testMultipleWrites() throws IOException {
    PersistableSlidingWindow slidingWindow = new PersistableSlidingWindow(1, TimeUnit.SECONDS, persistFile);
    long curTimestamp = Instant.now().toEpochMilli();
    slidingWindow.next(new SlidingWindowData(curTimestamp, 10));
    slidingWindow.write();
    slidingWindow.next(new SlidingWindowData(curTimestamp, 10));
    slidingWindow.write();
    PersistableSlidingWindow slidingWindow2 = new PersistableSlidingWindow(1, TimeUnit.SECONDS, persistFile);
    Assert.assertEquals(20, slidingWindow2.readSum(), 0.0);
  }

  @AfterClass
  public static void tearDown() {
    try {
      Files.deleteIfExists(persistFile);
      Files.deleteIfExists(testLocation);
    } catch (IOException e) {
      LOG.error("Couldn't delete file {}", persistFile, e);
    }
  }
}
