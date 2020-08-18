/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.categories.SlowTest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class SQLitePersistorTest {
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
    // FileUtils.cleanDirectory(tmpPath.toFile());
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
    // FileUtils.cleanDirectory(testLocation.toFile());
  }

  class TestRca extends Rca<ResourceFlowUnit> {
    public TestRca() {
      super(5);
    }

    @Override
    public ResourceFlowUnit operate() {
      return null;
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    }
  }

  @Test
  public void write() throws IOException, SQLException, InterruptedException {
    ResourceContext context = new ResourceContext(Resources.State.UNHEALTHY);
    HotResourceSummary summary =
        new HotResourceSummary(
            ResourceUtil.OLD_GEN_HEAP_USAGE,
            70,
            71,
            60);
    ResourceFlowUnit rfu = new ResourceFlowUnit(System.currentTimeMillis(), context, summary, true);

    Node rca = new TestRca();

    SQLitePersistor sqlite =
        new SQLitePersistor(
            testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);


    // The first write, this should create only one file as there is nothing to rotate.
    sqlite.write(rca, rfu);
    Assert.assertEquals(1,
        testLocation.toFile().list(new WildcardFileFilter(baseFilename + "*")).length);
    Assert.assertTrue(Paths.get(testLocation.toString(), baseFilename).toFile().exists());
    Thread.sleep(1000);

    // This should rotate the last file written and create a new one for this write.
    sqlite.write(rca, rfu);
    Assert.assertTrue(Paths.get(testLocation.toString(), baseFilename).toFile().exists());
    Assert.assertEquals(2, testLocation.toFile().list(new WildcardFileFilter(baseFilename + "*")).length);

    // This should delete the last rotated one, rotate the current sqlite and then write the data
    // in a new file. So the residual count on the directory should still be 2.
    Thread.sleep(1000);
    sqlite.write(rca, rfu);
    Assert.assertTrue(Paths.get(testLocation.toString(), baseFilename).toFile().exists());
    Assert.assertEquals(2, testLocation.toFile().list(new WildcardFileFilter(baseFilename + "*")).length);

    // A test to see that the string read from the database has the Rca name and the summary name
    // we expect.
    String readTableStr = sqlite.readTables();
    Assert.assertTrue(readTableStr.contains("TestRca"));
    Assert.assertTrue(readTableStr.contains("HotResourceSummary"));
  }

  @Test
  public void concurrentWriteAndRotate() throws IOException, SQLException {
    ResourceContext context = new ResourceContext(Resources.State.UNHEALTHY);
    HotResourceSummary summary =
        new HotResourceSummary(
            ResourceUtil.OLD_GEN_HEAP_USAGE,
            70,
            71,
            60);
    ResourceFlowUnit rfu = new ResourceFlowUnit(System.currentTimeMillis(), context, summary, true);
    Node rca = new TestRca();
    SQLitePersistor sqlite =
        new SQLitePersistor(
            testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.MICROSECONDS, 0);


    int numThreads = 100;
    int numWrites = 50;

    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      threads.add(new Thread(() -> {
        for (int j = 0; j < numWrites; j++) {
          try {
            sqlite.write(rca, rfu);
          } catch (SQLException | IOException throwables) {
            throwables.printStackTrace();
          }
        }
      }));
    }

    for (Thread th : threads) {
      th.start();
      try {
        th.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
