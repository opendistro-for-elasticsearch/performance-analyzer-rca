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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PeriodicSamplers implements Runnable {
  private static final Logger LOG = LogManager.getLogger(PeriodicSamplers.class);
  private final SampleAggregator aggregator;
  private final List<ISampler> allSamplers;
  private final ScheduledExecutorService executor;

  ScheduledFuture<?> future;

  public PeriodicSamplers(
      SampleAggregator aggregator, List<ISampler> samplers, long freq, TimeUnit timeUnit) {
    this.aggregator = aggregator;
    this.allSamplers = samplers;

    this.executor =
        Executors.newScheduledThreadPool(
            1, new ThreadFactoryBuilder().setNameFormat("resource-sampler-%d").build());
    this.future = this.executor.scheduleAtFixedRate(this, 0, freq, timeUnit);
    startExceptionHandlingThread();
  }

  @Override
  public void run() {
    for (ISampler sampler : allSamplers) {
      sampler.sample(aggregator);
    }
  }

  private void startExceptionHandlingThread() {
    new Thread(
            () -> {
              while (true) {
                try {
                  future.get();
                } catch (CancellationException cex) {
                  LOG.info("Periodic sampler cancellation requested.");
                } catch (Exception ex) {
                  LOG.error("Resource state poller exception cause : {}", ex.getCause());
                  ex.printStackTrace();
                }
              }
            })
        .start();
  }
}
