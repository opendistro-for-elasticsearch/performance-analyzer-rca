package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

public interface Resources {
  enum States implements Resources {
    HEALTHY,
    UN_HEALTHY,
    CONTENDED,
    STARVED,
    UNKNOWN
  }

  interface Types extends Resources {
    enum Network implements Types {
      TCP
    }

    enum Hardware implements Types {
      CPU,
      MEMORY,
      DISKS,
      NICS
    }

    enum OS implements Types {
      SCHEDULER,
      MEMORY_MANAGER
    }

    enum JVM implements Types {
      HEAP,
      GARBAGE_COLLECTOR,
      CODE,
      JIT
    }

    enum ElasticSearch implements Types {
      INDEXES,
      SHARDS,
      QUEUES,
      LOCKS,
      THREADS
    }
  }
}
