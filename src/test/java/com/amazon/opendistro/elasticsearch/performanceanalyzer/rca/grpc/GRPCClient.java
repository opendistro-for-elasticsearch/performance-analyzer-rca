package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class GRPCClient {
    private static final Logger LOG = LogManager.getLogger(GRPCClient.class);
    private Channel channel;
    private InterNodeRpcServiceGrpc.InterNodeRpcServiceBlockingStub client;
    private InterNodeRpcServiceGrpc.InterNodeRpcServiceStub asyncClient;

    public GRPCClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public GRPCClient(String host, int port, String rootCa, String cert, String privateKey) throws SSLException  {
        this(NettyChannelBuilder.forAddress(host, port).sslContext(buildSslContext(rootCa, cert, privateKey)));
    }

    public GRPCClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        client = InterNodeRpcServiceGrpc.newBlockingStub(channel);
        asyncClient = InterNodeRpcServiceGrpc.newStub(channel);
    }

    private static SslContext buildSslContext(String trustCertCollectionFilePath,
                                              String clientCertChainFilePath,
                                              String clientPrivateKeyFilePath) throws SSLException {
        SslContextBuilder builder = GrpcSslContexts.forClient();
        if (trustCertCollectionFilePath != null) {
            builder.trustManager(new File(trustCertCollectionFilePath));
        }
        if (clientCertChainFilePath != null && clientPrivateKeyFilePath != null) {
            builder.keyManager(new File(clientCertChainFilePath), new File(clientPrivateKeyFilePath));
        }
        return builder.build();
    }

    // Get Metrics https://opendistro.github.io/for-elasticsearch-docs/docs/pa/reference/
    public MetricsResponse getMetrics(MetricsRequest request) {
        return client.getMetrics(request);
    }

    // Request to subscribe to messages from an upstream node
    public SubscribeResponse subscribe(SubscribeMessage request) {
        return client.subscribe(request);
    }

    // Publish a series of FlowUnitMessages
    public PublishResponse publish(List<FlowUnitMessage> messages) throws InterruptedException {
        PublishResponseObserver publishObserver = new PublishResponseObserver();
        StreamObserver<FlowUnitMessage> responseObserver = asyncClient.publish(publishObserver);
        for (FlowUnitMessage message : messages) {
            responseObserver.onNext(message);
        }
        responseObserver.onCompleted();
        publishObserver.finishLatch.await(1, TimeUnit.MINUTES);
        return publishObserver.getResponse();
    }

    private class PublishResponseObserver implements StreamObserver<PublishResponse> {
        private PublishResponse response;
        final CountDownLatch finishLatch = new CountDownLatch(1);

        public PublishResponse getResponse() {
            return response;
        }

        @Override
        public void onNext(PublishResponse value) {
            this.response = value;
            LOG.info("Received PublishResponse: {}", value);
        }

        @Override
        public void onError(Throwable t) {
            LOG.error("Encountered error during subscribe calL.", t);
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {
            LOG.info("subscribe request completed successfully");
            finishLatch.countDown();
        }
    }
}
