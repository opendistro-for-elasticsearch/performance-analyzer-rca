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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

/**
 * The version of the RCA framework and the graphs combined. The version is a combination of the
 * major and the minor versions together.
 */
public final class Version {
  // This will hopefully never change.
  public static final String RCA_VERSION_STR = "rca-version";

  /**
   * An increase in the major version means that the flow units are in-compatible and if different
   * instances(physical nodes) are running different versions of the framework, then the transferred
   * packets should be dropped. Every increment here should be accompanied with a line of note.
   */
  final class Major {
    static final int RCA_MAJ_VERSION = 0;
  }

  /**
   * This is expected to increment with each noticeable change in the framework and also with
   * addition of new RCAs or enhancement of the old ones. But given this, we don't expect the minor
   * version to change for every single release and each increment should have a line stating what
   * changed.
   */
  final class Minor {
    static final String RCA_MINOR_VERSION = ".0.1";
  }

  /** @return The version string. */
  public static String getRcaVersion() {
    return Major.RCA_MAJ_VERSION + Minor.RCA_MINOR_VERSION;
  }
}
