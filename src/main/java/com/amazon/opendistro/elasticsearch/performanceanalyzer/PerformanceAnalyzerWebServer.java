/*
 * Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer;


import com.google.common.annotations.VisibleForTesting;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.Security;
import java.util.concurrent.Executors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class PerformanceAnalyzerWebServer {

    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerWebServer.class);

    @VisibleForTesting
    public static final String WEBSERVICE_BIND_HOST_NAME = "webservice-bind-host";
    // Use system default for max backlog.
    private static final int INCOMING_QUEUE_LENGTH = 1;

    public static HttpServer createInternalServer(
            int webServerPort, String hostFromSetting, boolean httpsEnabled) {
        try {
            Security.addProvider(new BouncyCastleProvider());
            HttpServer server;
            if (httpsEnabled) {
                server = createHttpsServer(webServerPort, hostFromSetting);
            } else {
                server = createHttpServer(webServerPort, hostFromSetting);
            }
            server.setExecutor(Executors.newCachedThreadPool());
            return server;
        } catch (java.net.BindException ex) {
            LOG.error("Could not create HttpServer on port {}", webServerPort, ex);
            Runtime.getRuntime().halt(1);
        } catch (Exception ex) {
            LOG.error("Unable to create HttpServer", ex);
            Runtime.getRuntime().halt(1);
        }
        return null;
    }

    /**
     * ClientAuthConfigurator makes the server perform client authentication if the user has set up
     * a certificate authority
     */
    private static class ClientAuthConfigurator extends HttpsConfigurator {
        public ClientAuthConfigurator(SSLContext sslContext) {
            super(sslContext);
        }

        @Override
        public void configure(HttpsParameters params) {
            final SSLParameters sslParams = getSSLContext().getDefaultSSLParameters();
            if (CertificateUtils.getTrustedCasFile() != null) {
                LOG.debug("Enabling client auth");
                final SSLEngine sslEngine = getSSLContext().createSSLEngine();
                sslParams.setNeedClientAuth(true);
                sslParams.setCipherSuites(sslEngine.getEnabledCipherSuites());
                sslParams.setProtocols(sslEngine.getEnabledProtocols());
                params.setSSLParameters(sslParams);
            } else {
                LOG.debug("Not enabling client auth");
                super.configure(params);
            }
        }
    }

    private static HttpServer createHttpsServer(int readerPort, String bindHost) throws Exception {
        HttpsServer server;
        if (bindHost != null && !bindHost.trim().isEmpty()) {
            LOG.info("Binding to Interface: {}", bindHost);
            server =
                    HttpsServer.create(
                            new InetSocketAddress(
                                    InetAddress.getByName(bindHost.trim()), readerPort),
                            INCOMING_QUEUE_LENGTH);
        } else {
            LOG.info(
                    "Value Not Configured for: {} Using default value: binding only to local interface",
                    WEBSERVICE_BIND_HOST_NAME);
            server =
                    HttpsServer.create(
                            new InetSocketAddress(InetAddress.getLoopbackAddress(), readerPort),
                            INCOMING_QUEUE_LENGTH);
        }

        // Install the all-trusting trust manager
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");

        KeyStore ks = CertificateUtils.createKeyStore();
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("NewSunX509");
        kmf.init(ks, CertificateUtils.IN_MEMORY_PWD.toCharArray());
        sslContext.init(kmf.getKeyManagers(), CertificateUtils.getTrustManagers(true), null);
        server.setHttpsConfigurator(new ClientAuthConfigurator(sslContext));

        // TODO ask ktkrg why this is necessary
        // Try to set HttpsURLConnection defaults, our webserver can still run even if this block
        // fails
        try {
            LOG.debug("Setting default SSLSocketFactory...");
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            LOG.debug("Default SSLSocketFactory set successfully");
            HostnameVerifier allHostsValid = (hostname, session) -> true;
            LOG.debug("Setting default HostnameVerifier...");
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
            LOG.debug("Default HostnameVerifier set successfully");
        } catch (Exception e) { // Usually AccessControlException
            LOG.warn("Exception while trying to set URLConnection defaults", e);
        }

        return server;
    }

    private static HttpServer createHttpServer(int readerPort, String bindHost) throws Exception {
        HttpServer server = null;
        if (bindHost != null && !bindHost.trim().isEmpty()) {
            LOG.info("Binding to Interface: {}", bindHost);
            server =
                    HttpServer.create(
                            new InetSocketAddress(
                                    InetAddress.getByName(bindHost.trim()), readerPort),
                            INCOMING_QUEUE_LENGTH);
        } else {
            LOG.info(
                    "Value Not Configured for: {} Using default value: binding only to local interface",
                    WEBSERVICE_BIND_HOST_NAME);
            server =
                    HttpServer.create(
                            new InetSocketAddress(InetAddress.getLoopbackAddress(), readerPort),
                            INCOMING_QUEUE_LENGTH);
        }

        return server;
    }
}
