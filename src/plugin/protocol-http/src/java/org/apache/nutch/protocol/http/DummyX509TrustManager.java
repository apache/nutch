/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Based on EasyX509TrustManager from commons-httpclient.
 */
package org.apache.nutch.protocol.http;

import java.lang.invoke.MethodHandles;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyX509TrustManager implements X509TrustManager {
  private X509TrustManager standardTrustManager = null;

  /** Logger object for this class. */
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Constructor for DummyX509TrustManager.
   * @param keystore a initialized {@link java.security.KeyStore}
   * @throws NoSuchAlgorithmException if there is a fatal error obtaining a
   * {@link javax.net.ssl.TrustManagerFactory} for the default algorithm.
   * @see TrustManagerFactory#getDefaultAlgorithm()
   * @throws KeyStoreException if there is a fatal error initializing the 
   * {@link javax.net.ssl.TrustManagerFactory} with the provided keystore.
   */
  public DummyX509TrustManager(KeyStore keystore)
      throws NoSuchAlgorithmException, KeyStoreException {
    super();
    String algo = TrustManagerFactory.getDefaultAlgorithm();
    TrustManagerFactory factory = TrustManagerFactory.getInstance(algo);
    factory.init(keystore);
    TrustManager[] trustmanagers = factory.getTrustManagers();
    if (trustmanagers.length == 0) {
      throw new NoSuchAlgorithmException(algo + " trust manager not supported");
    }
    this.standardTrustManager = (X509TrustManager) trustmanagers[0];
  }

  /**
   * @see javax.net.ssl.X509TrustManager#checkClientTrusted(X509Certificate[],
   *      String)
   * @param certificates a {@link java.security.cert.X509Certificate} array
   * @return true if trusted, false otherwise
   */
  public boolean isClientTrusted(X509Certificate[] certificates) {
    return true;
  }

  /**
   * @see javax.net.ssl.X509TrustManager#checkServerTrusted(X509Certificate[],
   *      String)
   * @param certificates a {@link java.security.cert.X509Certificate} array
   * @return true if trusted, false otherwise
   */
  public boolean isServerTrusted(X509Certificate[] certificates) {
    return true;
  }

  /**
   * @see javax.net.ssl.X509TrustManager#getAcceptedIssuers()
   * @return a {@link java.security.cert.X509Certificate} array
   */
  public X509Certificate[] getAcceptedIssuers() {
    return this.standardTrustManager.getAcceptedIssuers();
  }

  public void checkClientTrusted(X509Certificate[] arg0, String arg1)
      throws CertificateException {
    // do nothing

  }

  public void checkServerTrusted(X509Certificate[] arg0, String arg1)
      throws CertificateException {
    // do nothing

  }
}
