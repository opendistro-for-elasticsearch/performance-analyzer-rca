package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;

public interface EventProcessor {
  int BATCH_LIMIT = 500;

  void initializeProcessing(long startTime, long endTime);

  void finalizeProcessing();

  void processEvent(Event event);

  boolean shouldProcessEvent(Event event);

  void commitBatchIfRequired();
}
