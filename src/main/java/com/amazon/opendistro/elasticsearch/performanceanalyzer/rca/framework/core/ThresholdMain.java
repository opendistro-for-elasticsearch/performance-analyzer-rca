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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedThresholdFile;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class ThresholdMain {

  private ConcurrentMap<String, Threshold> thresholdMap;
  private final String PATH_TO_THRESHOLD_DIR;
  private AtomicLong lastThresholdCheckEpochMillis = new AtomicLong();
  private final long THRESHOLD_CHECK_INTERVAL_MILLIS;

  private static final long MINUTES_TO_MILLIS = 60 * 1000;
  private static final String THRESHOLD_FILE_EXTENSION = ".json";

  public ThresholdMain(String pathToThresholdDir, RcaConf rcaConf) {
    this.thresholdMap = new ConcurrentHashMap<>();
    this.PATH_TO_THRESHOLD_DIR = pathToThresholdDir;
    this.lastThresholdCheckEpochMillis.set(System.currentTimeMillis());
    this.THRESHOLD_CHECK_INTERVAL_MILLIS =
        rcaConf.getNewThresholdCheckPeriodicityMins() * MINUTES_TO_MILLIS;
    this.lastThresholdCheckEpochMillis.set(0);
  }

  private Threshold addNewThreshold(String thresholdName)
      throws IOException, MalformedThresholdFile {
    JsonFactory factory = new JsonFactory();
    factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
    ObjectMapper mapper = new ObjectMapper(factory);

    Path path = Paths.get(PATH_TO_THRESHOLD_DIR, thresholdName + THRESHOLD_FILE_EXTENSION);
    Threshold th = mapper.readValue(path.toFile(), Threshold.class);
    th.setPathToFile(path.toString());
    th.setCreationTime(lastThresholdCheckEpochMillis.get());

    th.validate();

    thresholdMap.put(thresholdName, th);
    return th;
  }

  /**
   * Get the value of the threshold considering the overrides, based on the runtime.
   *
   * <p>A threshold can have multiple overrides which need to be evaluated in order to come up with
   * the appropriate value of the threshold. This method can be called multiple times to get the
   * value of a threshold and therefore, the value of the threshold is cached. When the threshold is
   * asked for the first time, the threshold file with the same name as the threshold is searched
   * for in the threshold directory. If the file is not found, then IOException is thrown. If the
   * file is found, then it is parsed using Jackson library and the simple POJO returned as an
   * instance of Threshold class is saved for usage later. One thing to notice is that the threshold
   * creation time is set to the value of the current epoch of the Threshold time in milliseconds.
   * This is important. The Threshold store is implemented such that thresholds can be changed
   * dynamically and the changes should be picked up without a process restart. This is where the
   * creation time comes into play. When the threshold is accessed for the first time, the creation
   * time is set to the current epoch. When queried, the second time around, we check for the expiry
   * of the newThresholdCheck interval. If it has expired, then we check whether the underlying json
   * file this threshold is built from, has also been modified, since the object creation time. If
   * so, then we remove the last instance of the threshold and create a new instance of it and add
   * the new instance to the cache. This new instance is returned this time. Its is worth nothing
   * that even if multiple threshold files might have been modified since their objects were last
   * created, only the one asked for in this call is re-newed. This is done on purpose as we don't
   * want this call to take the hit of reading and instantiating all the newly updated thresholds.
   * But please notice that all such updated thresholds will get updated in due course when they are
   * queried for next. Another thing to note is that an update to the threshold happens only after
   * the expiry of the new-threshold-check-minutes config in the rca.conf. Till that happens, the
   * stale value will be served.
   *
   * <p>Notes on concurrent access: One thing to note is that during the RCA execution, multiple
   * threads can concurrently ask for the same threshold and if that threshold does not exist, all
   * of them will try to add it to the map. Therefore, the access to the map should be synchronized.
   * A java long can be longer than processor word length and therefore, it is also made atomic. But
   * notice that methods like get() and addNewThreshold() don't have synchronized blocks although
   * one might argue that, two threads can both query for a threshold and not find it, or find that
   * it was modified since it was last read, and both of them will create their own instances of the
   * Threshold and then update the same in the cache. Because the map is concurrent, one will
   * overwrite the other. We are not concerned by that as both of them will read from the same file
   * on disk and create the same instance of the threshold and overwriting will change the ref but
   * not the data the threshold contains. That's a performance overhead but note that it only
   * happens around the time when the THRESHOLD_CHECK_INTERVAL_MILLIS expires; which is much more
   * in-frequent than the frequency at which the threshold's get() method is called. Having a
   * synchronized block there will hurt us more than any potential gain.
   *
   * @param thresholdName The name of the threshold whose value is desired.
   * @return The value of the threshold after override evaluation based on the where this is being
   *     run.
   * @throws IOException if the filename corresponding to the threshold is not found.
   * @throws MalformedThresholdFile If the threshold file is not what is expected. The reason this
   *     can be thrown are many fold: 1. Mandatory fields don't exist. Mandatory fields are the name
   *     and default value. 2. Every override should find its mention in the override-precedence
   *     list. Otherwise, we shall have an override and we would not know how to prioritize it. 3.
   *     the key names of the overrides and the once mentioned in the precedence-list must match. 4.
   *     Overrides and their precedence order must both be present or neither must be present.
   */
  public String get(String thresholdName, RcaConf rcaConf)
      throws IOException, MalformedThresholdFile {
    Threshold th = thresholdMap.get(thresholdName);
    long currTimeMillis = 0;

    boolean timeToUpdate = false;

    if (lastThresholdCheckEpochMillis.get() == 0) {
      currTimeMillis = System.currentTimeMillis();
      lastThresholdCheckEpochMillis.set(currTimeMillis);
    } else if (currTimeMillis - lastThresholdCheckEpochMillis.get()
        >= THRESHOLD_CHECK_INTERVAL_MILLIS) {
      currTimeMillis = System.currentTimeMillis();
      lastThresholdCheckEpochMillis.set(currTimeMillis);
      timeToUpdate = true;
    }

    if (th == null) {
      // This is the first time the threshold was asked for. Let's create it.
      th = addNewThreshold(thresholdName);
    } else {
      // This threshold is an old one we were working with. If we are passed the threshold check
      // interval,
      // we want to check if the last modified time of the file, from which this threshold is
      // obtained has been
      // updated. If so, we would like to update the threshold.
      if (timeToUpdate) {
        long lastModified = new File(th.getPathToFile()).lastModified();
        if (lastModified > th.getCreationTime()) {
          // The file was modified since it was last read, so we have to update.
          thresholdMap.remove(thresholdName);
          th = addNewThreshold(thresholdName);
        }
      }
    }
    return th.get(rcaConf);
  }
}
