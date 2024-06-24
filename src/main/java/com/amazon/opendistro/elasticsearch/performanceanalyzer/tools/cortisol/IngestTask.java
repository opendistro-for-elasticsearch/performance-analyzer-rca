package com.amazon.opendistro.elasticsearch.performanceanalyzer.tools.cortisol;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class IngestTask implements Runnable {
    private static final String CORTISOL_PREFIX = "cort-";
    private final String endpoint;
    private final int port;
    private final BulkLoadParams params;
    private final long endTime;
    private final RestHighLevelClient client;
    private final Random random;
    private final String indexName;
    private final TokenBucket tokenBucket;
    private final AtomicInteger requestCount;
    private final AtomicInteger successCount;
    private final AtomicInteger failureCount;

    public IngestTask(final BulkLoadParams bulkLoadParams, final String endpoint, final int port, final int nIngestThreads) {
        this.params = bulkLoadParams;
        this.endpoint = endpoint;
        this.port = port;
        long startTime = System.currentTimeMillis();
        this.endTime = startTime + bulkLoadParams.getDurationInSeconds() * 1000;
        this.client = buildEsClient();
        this.random = new Random();
        this.requestCount = new AtomicInteger();
        this.successCount = new AtomicInteger();
        this.failureCount = new AtomicInteger();
        this.indexName = CORTISOL_PREFIX + random.nextInt(100);
        this.tokenBucket = new TokenBucket(params.getDocsPerSecond() / (params.getDocsPerRequest() * nIngestThreads));
    }

    @Override
    public void run() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(params.getTrackLocation()));
            createIndex(params.getMappingTemplateJson(), 3, 1);

            while (System.currentTimeMillis() < endTime) {
                // take blocks till tokens are available in the token bucket.
                // 0 indicates an interrupted exception, so break out and exit.
                if (tokenBucket.take() != 0) {
                    List<String> docs = new ArrayList<>();
                    for (int i = 0; i < params.getDocsPerRequest(); ++i) {
                        String doc = br.readLine();
                        if (doc == null) {
                            // restart the indexing from the top.
                            br = new BufferedReader(new FileReader(params.getTrackLocation()));
                            doc = br.readLine();
                        }

                        docs.add(doc);
                    }
                    makeBulkRequest(docs);
                } else {
                    break;
                }
            }

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private RestHighLevelClient buildEsClient() {
        final RestClientBuilder builder = RestClient.builder(new HttpHost(endpoint, port, "http"));
        return new RestHighLevelClient(builder);
    }

    private void createIndex(final String mappingTemplateJson, int nPrimaryShards, int nReplicaShards) throws IOException {
        final CreateIndexRequest cir = new CreateIndexRequest(indexName);
        cir.mapping(mappingTemplateJson, XContentType.JSON);
        cir.settings(CortisolHelper.buildIndexSettingsJson(nPrimaryShards, nReplicaShards), XContentType.JSON);
        final CreateIndexResponse response = client.indices().create(cir, RequestOptions.DEFAULT);
        assert response.isAcknowledged();
    }

    private void makeBulkRequest(final List<String> bulkDocs) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (String bulkDoc : bulkDocs) {
            final IndexRequest indexRequest = Requests.indexRequest(indexName);
            indexRequest.source(bulkDoc, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                if (!bulkItemResponses.hasFailures()) {
                    successCount.addAndGet(bulkDocs.size());
                } else {
                    for (BulkItemResponse response : bulkItemResponses) {
                        if (response.isFailed()) {
                            failureCount.incrementAndGet();
                        } else {
                            successCount.incrementAndGet();
                        }
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.addAndGet(bulkDocs.size());
            }
        });
        requestCount.addAndGet(bulkDocs.size());
    }

    /**
     * Custom token bucket class that lets at most {@code nTokens} be available every second.
     */
    private class TokenBucket {
        private final Lock lock = new ReentrantLock();
        private final Condition hasTokens = lock.newCondition();
        private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

        private final int maxTokens;
        private int nTokens;

        public TokenBucket(final int maxTokens) {
            this.maxTokens = maxTokens;
            this.ses.schedule(this::refill, 1, TimeUnit.SECONDS);
        }

        public int take() {
            lock.lock();
            try {
                while (nTokens == 0) {
                    hasTokens.await();
                }
                return nTokens--;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }

            return 0;
        }

        private void refill() {
            lock.lock();
            try {
                nTokens = maxTokens;
                hasTokens.signal();
            } finally {
                lock.unlock();
            }
        }
    }
}
