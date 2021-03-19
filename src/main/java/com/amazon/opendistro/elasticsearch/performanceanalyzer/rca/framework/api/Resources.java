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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

public class Resources {

  public interface ResourceType {

  }

  public enum Network implements ResourceType {
    TCP(Constants.TCP_VALUE);

    private final String value;

    Network(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {

      public static final String TCP_VALUE = "TCP";
    }
  }

  public enum Hardware implements ResourceType {
    CPU(Constants.CPU_VALUE),
    MEMORY(Constants.MEMORY_VALUE),
    DISKS(Constants.DISKS_VALUE),
    NICS(Constants.NICS_VALUE);

    private final String value;

    Hardware(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {

      public static final String CPU_VALUE = "cpu";
      public static final String MEMORY_VALUE = "memory";
      public static final String DISKS_VALUE = "disk";
      public static final String NICS_VALUE = "nic";
    }
  }

  public enum OS implements ResourceType {
    SCHEDULER(Constants.SCHEDULER_VALUE),
    MEMORY_MANAGER(Constants.MM_VALUE);

    private final String value;

    OS(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {

      public static final String SCHEDULER_VALUE = "scheduler";
      public static final String MM_VALUE = "memory manager";
    }
  }

  public enum JVM implements ResourceType {
    HEAP(Constants.HEAP_VALUE),
    GARBAGE_COLLECTOR(Constants.GC_VALUE),
    CODE(Constants.CODE_VALUE),
    JIT(Constants.JIT_VALUE),
    OLD_GEN(Constants.OLD_GEN_VALUE),
    YOUNG_GEN(Constants.YOUNG_GEN_VALUE);

    private final String value;

    JVM(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {

      public static final String HEAP_VALUE = "heap";
      public static final String GC_VALUE = "garbage collector";
      public static final String CODE_VALUE = "code";
      public static final String JIT_VALUE = "JIT";
      public static final String OLD_GEN_VALUE = "old generation";
      public static final String YOUNG_GEN_VALUE = "young generation";
    }
  }

  public enum ElasticSearch implements ResourceType {
    INDEXES(Constants.INDEXES_VALUE),
    SHARDS(Constants.SHARDS_VALUE),
    QUEUES(Constants.QUEUES_VALUE),
    LOCKS(Constants.LOCKS_VALUE),
    THREADS(Constants.THREADS_VALUE);

    private final String value;

    ElasticSearch(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {

      public static final String INDEXES_VALUE = "indexes";
      public static final String SHARDS_VALUE = "shards";
      public static final String QUEUES_VALUE = "queues";
      public static final String LOCKS_VALUE = "locks";
      public static final String THREADS_VALUE = "threads";
    }
  }

  public enum State {
    HEALTHY(Constants.HEALTHY_VALUE),
    UNHEALTHY(Constants.UNHEALTHY_VALUE),
    CONTENDED(Constants.CONTENDED_VALUE),
    STARVED(Constants.STARVED_VALUE),
    UNKNOWN(Constants.UNKOWN_VALUE);

    private final String value;

    State(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {

      public static final String HEALTHY_VALUE = "healthy";
      public static final String UNHEALTHY_VALUE = "unhealthy";
      public static final String CONTENDED_VALUE = "contended";
      public static final String STARVED_VALUE = "starved";
      public static final String UNKOWN_VALUE = "unknown";
    }
  }
}
