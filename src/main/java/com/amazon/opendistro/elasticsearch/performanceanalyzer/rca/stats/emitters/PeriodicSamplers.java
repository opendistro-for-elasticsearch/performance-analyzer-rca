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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PeriodicSamplers implements Runnable {
  private static final Logger LOG = LogManager.getLogger(PeriodicSamplers.class);

  private final SampleAggregator aggregator;
  private final List<ISampler> allSamplers;
  private final ScheduledFuture<?> samplerHandle;
  private final AtomicBoolean running = new AtomicBoolean(false);

  public PeriodicSamplers(SampleAggregator aggregator, List<ISampler> samplers, long freq, TimeUnit timeUnit) {
    this.aggregator = aggregator;
    this.allSamplers = samplers;
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(
            1, new ThreadFactoryBuilder().setNameFormat("resource-sampler-%d").build());
    this.samplerHandle = executor.scheduleAtFixedRate(this, 0, freq, timeUnit);
  }

  @Override
  public void run() {
    for (ISampler sampler : allSamplers) {
      try {
        sampler.sample(aggregator);
      } catch (Exception ex) {
        LOG.error("Sampler {} encountered an exception during sampling", sampler.getClass().getSimpleName(), ex);
      }
    }
  }

  public ScheduledFuture<?> getSamplerHandle() {
    return samplerHandle;
  }

  /**
   * Does nothing until the scheduled task referenced by {@link PeriodicSamplers#samplerHandle} is cancelled.
   * Once the task is cancelled, this function logs a message throws an {@link InterruptedException} which should
   * be handled by the parent thread.
   */
  void heartbeat() throws InterruptedException {
    if (samplerHandle.isCancelled()) {
      LOG.info("Periodic sampler cancellation requested.");
      throw new InterruptedException();
    }
    Thread.sleep(1000L);
  }

  /**
   * Starts and returns a {@link Thread} which performs a "heartbeat" check on the start of the
   * {@link PeriodicSamplers#samplerHandle} every 1s
   * @return The {@link Thread} which is performing the heartbeat
   */
  public Thread startHeartbeat() {
    Thread t = new Thread(() -> {
      running.set(true);
      while (running.get()) {
        try {
          heartbeat();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    t.start();
    return t;
  }
}
