package com.amazon.opendistro.elasticsearch.performanceanalyzer.tools.cortisol;

import org.elasticsearch.client.RestHighLevelClient;

import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * A basic implementation of {@link CortisolClient}.
 */
public class SimpleCortisolClient implements CortisolClient {
    private final String endpoint;
    private final int port;
    private final int nThreads;
    private final ExecutorService executorService;

    public SimpleCortisolClient(final String endpoint, final int port) {
        this.endpoint = endpoint;
        this.port = port;
        nThreads = Runtime.getRuntime().availableProcessors();
        executorService = Executors.newFixedThreadPool(nThreads);
    }

    /**
     * Tries to run an ingest load as specified by the docsPerSecond configured in the {@link BulkLoadParams} instance.
     * The way it runs the ingest load is by creating multiple threads where each thread will create its own index on
     * the cluster and ingests into it.
     * @param bulkLoadParams The object specifying the parameters for the stress test.
     */
    @Override
    public void stressBulk(BulkLoadParams bulkLoadParams) {
        setupShutDownHook(bulkLoadParams.getDurationInSeconds());
        IntStream.of(nThreads).forEach(i -> executorService.submit(new IngestTask(bulkLoadParams, endpoint, port, nThreads)));
    }

    @Override
    public void stressSearch(SearchLoadParams searchLoadParams) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void cleanup() {
        try {
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            executorService.shutdownNow();
        }
    }

    private void setupShutDownHook(final int duration) {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.schedule(this::cleanup, duration, TimeUnit.SECONDS);
        System.out.println("Shutdown hook enabled. Will shutdown after");
    }
}
