/*
 * Based on EasySSLProtocolSocketFactory from commons-httpclient:
 * 
 * $Header:
 * /home/jerenkrantz/tmp/commons/commons-convert/cvs/home/cvs/jakarta-commons//httpclient/src/contrib/org/apache/commons/httpclient/contrib/ssl/DummySSLProtocolSocketFactory.java,v
 * 1.7 2004/06/11 19:26:27 olegk Exp $ $Revision$ $Date: 2005-02-26 05:01:52
 * -0800 (Sat, 26 Feb 2005) $
 */

package org.apache.nutch.protocol.httpclient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.HttpClientError;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.ControllerThreadSocketFactory;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sun.net.ssl.SSLContext;
import com.sun.net.ssl.TrustManager;

public class DummySSLProtocolSocketFactory implements ProtocolSocketFactory {

  /** Log object for this class. */
  private static final Log LOG = LogFactory.getLog(DummySSLProtocolSocketFactory.class);

  private SSLContext sslcontext = null;

  /**
   * Constructor for DummySSLProtocolSocketFactory.
   */
  public DummySSLProtocolSocketFactory() {
    super();
  }

  private static SSLContext createEasySSLContext() {
    try {
      SSLContext context = SSLContext.getInstance("SSL");
      context.init(null, new TrustManager[] { new DummyX509TrustManager(null) }, null);
      return context;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new HttpClientError(e.toString());
    }
  }

  private SSLContext getSSLContext() {
    if (this.sslcontext == null) {
      this.sslcontext = createEasySSLContext();
    }
    return this.sslcontext;
  }

  /**
   * @see org.apache.commons.httpclient.protocol.SecureProtocolSocketFactory#createSocket(String,int,InetAddress,int)
   */
  public Socket createSocket(String host, int port, InetAddress clientHost, int clientPort) throws IOException,
          UnknownHostException {

    return getSSLContext().getSocketFactory().createSocket(host, port, clientHost, clientPort);
  }

  /**
   * Attempts to get a new socket connection to the given host within the given
   * time limit.
   * <p>
   * To circumvent the limitations of older JREs that do not support connect
   * timeout a controller thread is executed. The controller thread attempts to
   * create a new socket within the given limit of time. If socket constructor
   * does not return until the timeout expires, the controller terminates and
   * throws an {@link ConnectTimeoutException}
   * </p>
   * 
   * @param host the host name/IP
   * @param port the port on the host
   * @param localAddress the local host name/IP to bind the socket to
   * @param localPort the port on the local machine
   * @param params {@link HttpConnectionParams Http connection parameters}
   * 
   * @return Socket a new socket
   * 
   * @throws IOException if an I/O error occurs while creating the socket
   * @throws UnknownHostException if the IP address of the host cannot be
   *         determined
   */
  public Socket createSocket(final String host, final int port, final InetAddress localAddress, final int localPort,
          final HttpConnectionParams params) throws IOException, UnknownHostException, ConnectTimeoutException {
    if (params == null) {
      throw new IllegalArgumentException("Parameters may not be null");
    }
    int timeout = params.getConnectionTimeout();
    if (timeout == 0) {
      return createSocket(host, port, localAddress, localPort);
    } else {
      // To be eventually deprecated when migrated to Java 1.4 or above
      return ControllerThreadSocketFactory.createSocket(this, host, port, localAddress, localPort, timeout);
    }
  }

  /**
   * @see org.apache.commons.httpclient.protocol.SecureProtocolSocketFactory#createSocket(String,int)
   */
  public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
    return getSSLContext().getSocketFactory().createSocket(host, port);
  }

  /**
   * @see org.apache.commons.httpclient.protocol.SecureProtocolSocketFactory#createSocket(Socket,String,int,boolean)
   */
  public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException,
          UnknownHostException {
    return getSSLContext().getSocketFactory().createSocket(socket, host, port, autoClose);
  }

  public boolean equals(Object obj) {
    return ((obj != null) && obj.getClass().equals(DummySSLProtocolSocketFactory.class));
  }

  public int hashCode() {
    return DummySSLProtocolSocketFactory.class.hashCode();
  }

}
