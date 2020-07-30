/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import java.io.File;
import java.io.FileReader;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.annotation.Nullable;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;

public class CertificateUtils {

  public static final String ALIAS_IDENTITY = "identity";
  public static final String ALIAS_CERT = "cert";
  // The password is not used to encrypt keys on disk.
  public static final String IN_MEMORY_PWD = "opendistro";
  public static final String CERTIFICATE_FILE_PATH = "certificate-file-path";
  public static final String PRIVATE_KEY_FILE_PATH = "private-key-file-path";
  public static final String TRUSTED_CAS_FILE_PATH = "trusted-cas-file-path";
  public static final String CLIENT_PREFIX = "client-";
  public static final String CLIENT_CERTIFICATE_FILE_PATH = CLIENT_PREFIX + CERTIFICATE_FILE_PATH;
  public static final String CLIENT_PRIVATE_KEY_FILE_PATH = CLIENT_PREFIX + PRIVATE_KEY_FILE_PATH;
  public static final String CLIENT_TRUSTED_CAS_FILE_PATH = CLIENT_PREFIX + TRUSTED_CAS_FILE_PATH;

  private static final Logger LOGGER = LogManager.getLogger(CertificateUtils.class);

  public static Certificate getCertificate(final FileReader certReader) throws Exception {
    try (PEMParser pemParser = new PEMParser(certReader)) {
      X509CertificateHolder certificateHolder = (X509CertificateHolder) pemParser.readObject();
      Certificate caCertificate =
          new JcaX509CertificateConverter().setProvider("BC").getCertificate(certificateHolder);
      return caCertificate;
    }
  }

  public static PrivateKey getPrivateKey(final FileReader keyReader) throws Exception {
    try (PEMParser pemParser = new PEMParser(keyReader)) {
      PrivateKeyInfo pki = (PrivateKeyInfo) pemParser.readObject();
      return BouncyCastleProvider.getPrivateKey(pki);
    }
  }

  public static KeyStore createKeyStore() throws Exception {
    String certFilePath = PluginSettings.instance().getSettingValue(CERTIFICATE_FILE_PATH);
    String keyFilePath = PluginSettings.instance().getSettingValue(PRIVATE_KEY_FILE_PATH);
    KeyStore.ProtectionParameter protParam = new KeyStore.PasswordProtection(
            CertificateUtils.IN_MEMORY_PWD.toCharArray());
    PrivateKey pk = getPrivateKey(new FileReader(keyFilePath));
    KeyStore ks = createEmptyStore();
    Certificate certificate = getCertificate(new FileReader(certFilePath));
    ks.setEntry(ALIAS_IDENTITY, new KeyStore.PrivateKeyEntry(pk, new Certificate[]{certificate}), protParam);
    return ks;
  }

  public static TrustManager[] getTrustManagers(boolean forServer) throws Exception {
    // If a certificate authority is specified, create an authenticating trust manager
    String certificateAuthority;
    if (forServer) {
      certificateAuthority = PluginSettings.instance().getSettingValue(TRUSTED_CAS_FILE_PATH);
    } else {
      certificateAuthority = PluginSettings.instance().getSettingValue(CLIENT_TRUSTED_CAS_FILE_PATH);
    }
    if (certificateAuthority != null && !certificateAuthority.isEmpty()) {
      KeyStore ks = createEmptyStore();
      Certificate certificate = getCertificate(new FileReader(certificateAuthority));
      ks.setCertificateEntry(ALIAS_CERT, certificate);
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      return tmf.getTrustManagers();
    }
    // Otherwise, return an all-trusting TrustManager
    return new TrustManager[] {
            new X509TrustManager() {

              public X509Certificate[] getAcceptedIssuers() {
                return null;
              }

              public void checkClientTrusted(X509Certificate[] certs, String authType) {}

              public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }
    };
  }

  public static KeyStore createEmptyStore() throws Exception {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, IN_MEMORY_PWD.toCharArray());
    return ks;
  }

  public static File getCertificateFile() {
    String certFilePath = PluginSettings.instance().getSettingValue(CERTIFICATE_FILE_PATH);
    return new File(certFilePath);
  }

  public static File getPrivateKeyFile() {
    String privateKeyPath = PluginSettings.instance().getSettingValue(PRIVATE_KEY_FILE_PATH);
    return new File(privateKeyPath);
  }

  @Nullable
  public static File getTrustedCasFile() {
    String trustedCasPath = PluginSettings.instance().getSettingValue(TRUSTED_CAS_FILE_PATH);
    if (trustedCasPath == null || trustedCasPath.isEmpty()) {
      return null;
    }
    return new File(trustedCasPath);
  }

  public static File getClientCertificateFile() {
    String certFilePath = PluginSettings.instance().getSettingValue(CLIENT_CERTIFICATE_FILE_PATH);
    if (certFilePath == null || certFilePath.isEmpty()) {
      return getCertificateFile();
    }
    return new File(certFilePath);
  }

  public static File getClientPrivateKeyFile() {
    String privateKeyPath = PluginSettings.instance().getSettingValue(CLIENT_PRIVATE_KEY_FILE_PATH);
    if (privateKeyPath == null || privateKeyPath.isEmpty()) {
      return getPrivateKeyFile();
    }
    return new File(privateKeyPath);
  }

  @Nullable
  public static File getClientTrustedCasFile() {
    String trustedCasPath = PluginSettings.instance().getSettingValue(CLIENT_TRUSTED_CAS_FILE_PATH);
    // By default, use the same CA as the server
    if (trustedCasPath == null || trustedCasPath.isEmpty()) {
      return getTrustedCasFile();
    }
    return new File(trustedCasPath);
  }
}
