/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CertificateUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TestNetServer is a NetServer that clients can check is running or not
public class TestNetServer extends NetServer implements Runnable {
    private static final Logger LOG = LogManager.getLogger(TestNetServer.class);

    public AtomicBoolean isRunning = new AtomicBoolean(false);

    public TestNetServer(final int port, final int numServerThreads, final boolean useHttps) {
        super(port, numServerThreads, useHttps);
        // If we want TLS, use the certs in our test resources directory
        if (useHttps) {
            ClassLoader classLoader = getClass().getClassLoader();
            // TODO (sidnaray) this is not the best practice for overriding settings
            // CertificateUtils should be abstracted into an interface and should have a regular and testing impl
            // The interface should be passed into the NetServer constructor and have the proper impl "injected"
            PluginSettings.instance().overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH,
                    Objects.requireNonNull(classLoader.getResource("tls/rootca/RootCA.pem")).getFile());
            PluginSettings.instance().overrideProperty(CertificateUtils.CERTIFICATE_FILE_PATH,
                    Objects.requireNonNull(classLoader.getResource("tls/server/localhost.crt")).getFile());
            PluginSettings.instance().overrideProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH,
                    Objects.requireNonNull(classLoader.getResource("tls/server/localhost.key")).getFile());
        }
    }

    @Override
    public void getMetrics(MetricsRequest request, StreamObserver<MetricsResponse> responseObserver) {
        LOG.debug("MetricsRequest received by server! {}", request);
        responseObserver.onNext(MetricsResponse.newBuilder().setMetricsResult("metrics").build());
        responseObserver.onCompleted();
    }

    @Override
    protected void postStartHook() {
        isRunning.set(true);
    }

    @Override
    protected void shutdownHook() {
        isRunning.set(false);
    }
}
