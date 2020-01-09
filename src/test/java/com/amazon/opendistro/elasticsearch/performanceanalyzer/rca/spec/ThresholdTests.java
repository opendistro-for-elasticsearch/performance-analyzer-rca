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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedThresholdFile;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.OverridesAndPrecedenceOrderCountMismatch;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.OverridesAndPrecedenceOrderValueMismatch;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import java.io.IOException;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class ThresholdTests {
  private static final String confPath =
      Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString();
  private static final String thPath =
      Paths.get(RcaConsts.TEST_CONFIG_PATH, "thresholds").toString();

  RcaConf rcaConf;

  @Before
  public void init() {
    rcaConf = new RcaConf(confPath);
  }

  @Test
  public void testOverrideOrder() throws MalformedThresholdFile, IOException {
    String val = new ThresholdMain(thPath, rcaConf).get("test-threshold", rcaConf);
    assertEquals("val-ssd", val);
  }

  @Test(expected = MalformedThresholdFile.class)
  public void testValidationNoName() throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-no-name", rcaConf);
  }

  @Test(expected = MalformedThresholdFile.class)
  public void testValidationNoDefault() throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-no-default", rcaConf);
  }

  @Test(expected = MalformedThresholdFile.class)
  public void testValidationNameDiffersFromFilename() throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-name-different-from-filename", rcaConf);
  }

  @Test(expected = OverridesAndPrecedenceOrderValueMismatch.class)
  public void testValidationNoOverridesButPrecedenceOrder()
      throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-null-overrides", rcaConf);
  }

  @Test(expected = OverridesAndPrecedenceOrderValueMismatch.class)
  public void testValidationNoPrecedenceOrderButOverrides()
      throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-null-precedence-order", rcaConf);
  }

  @Test(expected = OverridesAndPrecedenceOrderValueMismatch.class)
  public void testValidationOverridesPrecedenceOrderKeyMismatch()
      throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-unmatched-overrides-precedence-order", rcaConf);
  }

  @Test(expected = OverridesAndPrecedenceOrderCountMismatch.class)
  public void testValidationOverridesPrecedenceOrderCountMismatch()
      throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-overrides-precedence-count-mismatch", rcaConf);
  }

  @Test
  public void testReturnDefaultWhenNoOverrides() throws IOException, MalformedThresholdFile {
    String val = new ThresholdMain(thPath, rcaConf).get("test-no-overrides", rcaConf);
    assertEquals("default-value", val);
  }

  @Test
  public void testDefaultNotString() throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-default-not-string", rcaConf);
  }

  @Test
  public void testOverriddenValueNotString() throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-override-not-string", rcaConf);
  }

  // TODO: Add tests for the threshold update when timer expires.
  @Test
  public void testThresholdUpdateOnFileModification() {}

  @Test
  public void testCommentsAllowed() throws IOException, MalformedThresholdFile {
    new ThresholdMain(thPath, rcaConf).get("test-comments", rcaConf);
  }
}
