package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.sun.net.httpserver.HttpServer;

import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpResponseStatus;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.util.Objects;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocketFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PerformanceAnalyzerWebServerTest {
    private static final String BIND_HOST = "localhost";
    private static final String PORT = "11021";
    private static final String MESSAGE = "hello";

    private String oldBindHost;
    private String oldPort;
    private String oldCertificateFilePath;
    private String oldPrivateKeyFilePath;
    private String oldTrustedCasFilePath;
    private String oldClientCertificateFilePath;
    private String oldClientPrivateKeyFilePath;
    private String oldClientTrustedCasFilePath;
    private boolean oldHttpsEnabled;

    private HttpServer server;

    @Before
    public void setup() {
        // Save old PluginSettings values
        oldBindHost = PluginSettings.instance().getSettingValue(PerformanceAnalyzerWebServer.WEBSERVICE_BIND_HOST_NAME);
        oldPort = PluginSettings.instance().getSettingValue(PerformanceAnalyzerWebServer.WEBSERVICE_PORT_CONF_NAME);
        oldCertificateFilePath = PluginSettings.instance().getSettingValue(CertificateUtils.CERTIFICATE_FILE_PATH);
        oldPrivateKeyFilePath = PluginSettings.instance().getSettingValue(CertificateUtils.PRIVATE_KEY_FILE_PATH);
        oldTrustedCasFilePath = PluginSettings.instance().getSettingValue(CertificateUtils.TRUSTED_CAS_FILE_PATH);
        oldClientCertificateFilePath = PluginSettings.instance().getSettingValue(CertificateUtils.CLIENT_CERTIFICATE_FILE_PATH);
        oldClientPrivateKeyFilePath = PluginSettings.instance().getSettingValue(CertificateUtils.CLIENT_PRIVATE_KEY_FILE_PATH);
        oldClientTrustedCasFilePath = PluginSettings.instance().getSettingValue(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH);
        oldHttpsEnabled = PluginSettings.instance().getHttpsEnabled();
        // Update bind host, port, and server certs for the test
        PluginSettings.instance().overrideProperty(PerformanceAnalyzerWebServer.WEBSERVICE_BIND_HOST_NAME, BIND_HOST);
        PluginSettings.instance().overrideProperty(PerformanceAnalyzerWebServer.WEBSERVICE_PORT_CONF_NAME, PORT);
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.CERTIFICATE_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/server/localhost.crt")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/server/localhost.key")).getFile());
    }

    @After
    public void tearDown() {
        // Unset all SSL settings
        if (oldBindHost != null) {
            PluginSettings.instance().overrideProperty(PerformanceAnalyzerWebServer.WEBSERVICE_BIND_HOST_NAME, oldBindHost);
        }
        if (oldPort != null) {
            PluginSettings.instance().overrideProperty(PerformanceAnalyzerWebServer.WEBSERVICE_PORT_CONF_NAME, oldPort);
        }
        if (oldCertificateFilePath != null) {
            PluginSettings.instance().overrideProperty(CertificateUtils.CERTIFICATE_FILE_PATH, oldCertificateFilePath);
        }
        if (oldPrivateKeyFilePath != null) {
            PluginSettings.instance().overrideProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH, oldPrivateKeyFilePath);
        }
        if (oldTrustedCasFilePath != null) {
            PluginSettings.instance().overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH, oldTrustedCasFilePath);
        }
        if (oldClientCertificateFilePath != null) {
            PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_CERTIFICATE_FILE_PATH, oldClientCertificateFilePath);
        }
        if (oldClientPrivateKeyFilePath != null) {
            PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_PRIVATE_KEY_FILE_PATH, oldClientPrivateKeyFilePath);
        }
        if (oldClientTrustedCasFilePath != null) {
            PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH, oldClientTrustedCasFilePath);
        }
        PluginSettings.instance().setHttpsEnabled(oldHttpsEnabled);

        // Stop the server
        if (server != null) {
            server.stop(0);
        }
    }

    public void initializeServer(boolean useHttps) {
        PluginSettings.instance().setHttpsEnabled(useHttps);
        server = PerformanceAnalyzerWebServer.createInternalServer(PORT, BIND_HOST, useHttps);
        Assert.assertNotNull(server);
        server.setExecutor(Executors.newFixedThreadPool(1));
        // Setup basic /test endpoint. When the server receives any request on /test, it responds with "hello"
        server.createContext("/test", exchange -> {
            exchange.getRequestBody().close();
            exchange.sendResponseHeaders(HttpResponseStatus.OK.code(), 0);
            OutputStream response = exchange.getResponseBody();
            response.write(MESSAGE.getBytes());
            response.close();
        });
        server.start();
    }

    /**
     * Issues a basic HTTP GET request to $BIND_HOST:$PORT/test and verifies that the response says "hello"
     */
    public void verifyRequest(String urlString) throws Exception {
        // Build the request
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/json");
        // Issue the request to the server
        int status = connection.getResponseCode();
        BufferedReader streamReader;
        if (status > 299) {
            streamReader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
        } else {
            streamReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        }
        // Read response & verify contents
        String inputLine;
        StringBuilder content = new StringBuilder();
        while ((inputLine = streamReader.readLine()) != null) {
            content.append(inputLine);
        }
        streamReader.close();
        Assert.assertEquals(MESSAGE, content.toString());
    }

    /**
     * Issues a basic HTTPS GET request to $BIND_HOST:$PORT/test and verifies that the response says "hello"
     */
    public void verifyHttpsRequest(String urlString, String clientCert, String clientKey, String clientCA)
            throws Exception {
        // Build the request
        URL url = new URL(urlString);
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setSSLSocketFactory(createSSLSocketFactory(clientCert, clientKey, clientCA));
        // Issue the request to the server
        int status = connection.getResponseCode();
        BufferedReader streamReader;
        if (status > 299) {
            streamReader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
        } else {
            streamReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        }
        // Read response & verify contents
        String inputLine;
        StringBuilder content = new StringBuilder();
        while ((inputLine = streamReader.readLine()) != null) {
            content.append(inputLine);
        }
        streamReader.close();
        Assert.assertEquals(MESSAGE, content.toString());
    }

    /**
     * testHttpServer verifies that any client can issue HTTP requests to the {@link PerformanceAnalyzerWebServer}
     * when TLS is disabled
     */
    @Test
    public void testHttpServer() throws Exception {
        // Start the HTTP server
        initializeServer(false);
        verifyRequest(String.format("http://%s:%s/test", BIND_HOST, PORT));
    }

    /**
     * Verifies that the server accepts any client's requests when HTTPS is enabled but Auth is disabled.
     */
    @Test
    public void testNoAuthHttps() throws Exception {
        PluginSettings.instance().overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH, "");
        initializeServer(true);
        verifyRequest(String.format("https://%s:%s/test", BIND_HOST, PORT));
    }

    /**
     * Utility method to create an {@link SSLSocketFactory}
     * @param clientCert Client identity certificate
     * @param clientKey Private key for the client certificate
     * @param clientCA Client certificate authority (used to verify server identity)
     *                 Set this to null if you don't want to authenticate the server
     * @return An {@link SSLSocketFactory} configured based on the given params
     * @throws Exception If something goes wrong with SSL setup
     */
    public SSLSocketFactory createSSLSocketFactory(String clientCert, String clientKey, @Nullable String clientCA) throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings instance = PluginSettings.instance();
        // Save previous settings
        String certFile = instance.getSettingValue(CertificateUtils.CERTIFICATE_FILE_PATH);
        String pKey = instance.getSettingValue(CertificateUtils.PRIVATE_KEY_FILE_PATH);
        String rootCA = instance.getSettingValue(CertificateUtils.TRUSTED_CAS_FILE_PATH);
        String prevClientCA = instance.getSettingValue(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH);
        // Override client identity settings
        instance.overrideProperty(CertificateUtils.CERTIFICATE_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource(clientCert)).getFile());
        instance.overrideProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource(clientKey)).getFile());
        instance.overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH, "");
        if (clientCA != null) {
            instance.getSettingValue(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH, clientCA);
        }
        // Setup SSLContext for the client
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        KeyStore ks = CertificateUtils.createKeyStore();
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("NewSunX509");
        kmf.init(ks, CertificateUtils.IN_MEMORY_PWD.toCharArray());
        sslContext.init(kmf.getKeyManagers(), CertificateUtils.getTrustManagers(false), null);
        // Restore previous settings
        instance.overrideProperty(CertificateUtils.CERTIFICATE_FILE_PATH,
                certFile);
        instance.overrideProperty(CertificateUtils.PRIVATE_KEY_FILE_PATH,
                pKey);
        instance.overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH, rootCA);
        if (prevClientCA == null) {
            instance.overrideProperty(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH, "");
        }
        return sslContext.getSocketFactory();
    }

    /**
     * Verifies that the HTTPS server responds to an authenticated client's requests.
     */
    @Test
    public void testAuthenticatedClientGetsResponse() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/rootca/RootCA.pem")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH,
            Objects.requireNonNull(classLoader.getResource("tls/rootca/RootCA.pem")).getFile());
        initializeServer(true);
        // Build the request
        verifyHttpsRequest(String.format("https://%s:%s/test", BIND_HOST, PORT),
                "tls/client/localhost.crt", "tls/client/localhost.key", "tls/rootca/RootCA.pem");
    }

    /**
     * Verifies that the HTTPS server doesn't respond to an unauthenticated client's requests.
     */
    @Test
    public void testUnauthenticatedClientGetsRejected() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/rootca/RootCA.pem")).getFile());
        initializeServer(true);
        // Build the request
        try {
            verifyHttpsRequest(String.format("https://%s:%s/test", BIND_HOST, PORT),
                    "tls/attacker/attack_cert.pem", "tls/attacker/attack_key.pem",
                    "tls/rootca/RootCA.pem");
            throw new AssertionError("An unauthenticated client was able to talk to the server");
        } catch (SSLException e) { // Unauthenticated client is rejected!
            assert true;
        } catch (Exception e) { // Treat unexpected errors as a failure
            throw new AssertionError("Received unexpected error when making unauthed REST call to server", e);
        }
    }

    /**
     * Verifies that a client properly authenticates a trusted server
     */
    @Test
    public void testClientAuth() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/rootca/RootCA.pem")).getFile());
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/rootca/RootCA.pem")).getFile());
        initializeServer(true);
        // Build the request
        verifyHttpsRequest(String.format("https://%s:%s/test", BIND_HOST, PORT),
                "tls/client/localhost.crt", "tls/client/localhost.key", "tls/rootca/RootCA.pem");
    }

    /**
     * Verifies that a client doesn't authenticate a server with unknown credentials
     */
    @Test
    public void testThatClientRejectsUntrustedServer() {
        ClassLoader classLoader = getClass().getClassLoader();
        PluginSettings.instance().overrideProperty(CertificateUtils.TRUSTED_CAS_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/rootca/RootCA.pem")).getFile());
        // Setup client CA that doesn't trust the server's identity
        PluginSettings.instance().overrideProperty(CertificateUtils.CLIENT_TRUSTED_CAS_FILE_PATH,
                Objects.requireNonNull(classLoader.getResource("tls/rootca/root2ca.pem")).getFile());
        initializeServer(true);
        // Build the request
        try {
            verifyHttpsRequest(String.format("https://%s:%s/test", BIND_HOST, PORT),
                    "tls/client/localhost.crt", "tls/client/localhost.key", "tls/rootca/root2ca.pem");
            throw new AssertionError("The client accepted a response from an untrusted server");
        } catch (SSLException e) { // Unauthenticated server is rejected!
            assert true;
        } catch (Exception e) { // Treat unexpected errors as a failure
            throw new AssertionError("Received unexpected error in testThatClientRejectsUntrustedServer", e);
        }
    }
}
