/**
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

package org.apache.nutch.protocol.ftp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.net.InetAddress;
import java.net.Socket;

import java.util.List;
//import java.util.LinkedList;

import org.apache.commons.net.MalformedServerReplyException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPCommand;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileEntryParser;
import org.apache.commons.net.ftp.FTPReply;

import org.apache.commons.net.ftp.FTPConnectionClosedException;

/***********************************************
 * Client.java encapsulates functionalities necessary for nutch to
 * get dir list and retrieve file from an FTP server.
 * This class takes care of all low level details of interacting
 * with an FTP server and provides a convenient higher level interface.
 *
 * Modified from FtpClient.java in apache commons-net.
 * 
 * Notes by John Xing:
 * ftp server implementations are hardly uniform and none seems to follow
 * RFCs whole-heartedly. We have no choice, but assume common denominator
 * as following:
 * (1) Use stream mode for data tranfer. Block mode will be better for
 *     multiple file downloading and partial file downloading. However
 *     not every ftpd has block mode support.
 * (2) Use passive mode for data connection.
 *     So nutch will work if we run behind firewall.
 * (3) Data connection is opened/closed per ftp command for the reasons
 *     listed in (1). There are ftp servers out there,
 *     when partial downloading is enforeced by closing data channel
 *     socket on our client side, the server side immediately closes
 *     control channel (socket). Our codes deal with such a bad behavior.
 * (4) LIST is used to obtain remote file attributes if possible.
 *     MDTM & SIZE would be nice, but not as ubiquitously implemented as LIST.
 * (5) Avoid using ABOR in single thread? Do not use it at all.
 *
 * About exceptions:
 * Some specific exceptions are re-thrown as one of FtpException*.java
 * In fact, each function throws FtpException*.java or pass IOException.
 *
 * @author John Xing
 ***********************************************/

public class Client extends FTP
{
    private int __dataTimeout;
    private int __passivePort;
    private String __passiveHost;
//    private int __fileType, __fileFormat;
    private boolean __remoteVerificationEnabled;
//    private FTPFileEntryParser __entryParser;
    private String __systemName;

    // constructor
    public Client()
    {
        __initDefaults();
        __dataTimeout = -1;
        __remoteVerificationEnabled = true;
    }

    // defaults when initialize
    private void __initDefaults()
    {
        __passiveHost        = null;
        __passivePort        = -1;
        __systemName         = null;
/*        __fileType           = FTP.ASCII_FILE_TYPE;
        __fileFormat         = FTP.NON_PRINT_TEXT_FORMAT;
        __entryParser        = null;
*/    }

    // parse reply for pass()
    private void __parsePassiveModeReply(String reply)
    throws MalformedServerReplyException
    {
        int i, index, lastIndex;
        String octet1, octet2;
        StringBuffer host;

        reply = reply.substring(reply.indexOf('(') + 1,
                                reply.indexOf(')')).trim();

        host = new StringBuffer(24);
        lastIndex = 0;
        index = reply.indexOf(',');
        host.append(reply.substring(lastIndex, index));

        for (i = 0; i < 3; i++)
        {
            host.append('.');
            lastIndex = index + 1;
            index = reply.indexOf(',', lastIndex);
            host.append(reply.substring(lastIndex, index));
        }

        lastIndex = index + 1;
        index = reply.indexOf(',', lastIndex);

        octet1 = reply.substring(lastIndex, index);
        octet2 = reply.substring(index + 1);

        // index and lastIndex now used as temporaries
        try
        {
            index = Integer.parseInt(octet1);
            lastIndex = Integer.parseInt(octet2);
        }
        catch (NumberFormatException e)
        {
            throw new MalformedServerReplyException(
                "Could not parse passive host information.\nServer Reply: " + reply);
        }

        index <<= 8;
        index |= lastIndex;

        __passiveHost = host.toString();
        __passivePort = index;
    }

    // open passive data connection socket
    protected Socket __openPassiveDataConnection(int command, String arg)
      throws IOException, FtpExceptionCanNotHaveDataConnection {
        Socket socket;

//        // 20040317, xing, accommodate ill-behaved servers, see below
//        int port_previous = __passivePort;

        if (pasv() != FTPReply.ENTERING_PASSIVE_MODE)
          throw new FtpExceptionCanNotHaveDataConnection(
            "pasv() failed. " + getReplyString());

        try {
          __parsePassiveModeReply(getReplyStrings()[0]);
        } catch (MalformedServerReplyException e) {
          throw new FtpExceptionCanNotHaveDataConnection(e.getMessage());
        }

//        // 20040317, xing, accommodate ill-behaved servers, see above
//        int count = 0;
//        System.err.println("__passivePort "+__passivePort);
//        System.err.println("port_previous "+port_previous);
//        while (__passivePort == port_previous) {
//          // just quit if too many tries. make it an exception here?
//          if (count++ > 10)
//            return null;
//          // slow down further for each new try
//          Thread.sleep(500*count);
//          if (pasv() != FTPReply.ENTERING_PASSIVE_MODE)
//            throw new FtpExceptionCanNotHaveDataConnection(
//              "pasv() failed. " + getReplyString());
//            //return null;
//          try {
//            __parsePassiveModeReply(getReplyStrings()[0]);
//          } catch (MalformedServerReplyException e) {
//            throw new FtpExceptionCanNotHaveDataConnection(e.getMessage());
//          }
//        }

        socket = _socketFactory_.createSocket(__passiveHost, __passivePort);

        if (!FTPReply.isPositivePreliminary(sendCommand(command, arg))) {
          socket.close();
          return null;
        }

        if (__remoteVerificationEnabled && !verifyRemote(socket))
        {
            InetAddress host1, host2;

            host1 = socket.getInetAddress();
            host2 = getRemoteAddress();

            socket.close();

            // our precaution
            throw new FtpExceptionCanNotHaveDataConnection(
                "Host attempting data connection " + host1.getHostAddress() +
                " is not same as server " + host2.getHostAddress() +
                " So we intentionally close it for security precaution."
                );
        }

        if (__dataTimeout >= 0)
            socket.setSoTimeout(__dataTimeout);

        return socket;
    }

    /***
     * Sets the timeout in milliseconds to use for data connection.
     * set immediately after opening the data connection.
     ***/
    public void setDataTimeout(int timeout)
    {
        __dataTimeout = timeout;
    }

    /***
     * Closes the connection to the FTP server and restores
     * connection parameters to the default values.
     * <p>
     * @exception IOException If an error occurs while disconnecting.
     ***/
    public void disconnect() throws IOException
    {
        __initDefaults();
        super.disconnect();
        // no worry for data connection, since we always close it
        // in every ftp command that invloves data connection
    }

    /***
     * Enable or disable verification that the remote host taking part
     * of a data connection is the same as the host to which the control
     * connection is attached.  The default is for verification to be
     * enabled.  You may set this value at any time, whether the
     * FTPClient is currently connected or not.
     * <p>
     * @param enable True to enable verification, false to disable verification.
     ***/
    public void setRemoteVerificationEnabled(boolean enable)
    {
        __remoteVerificationEnabled = enable;
    }

    /***
     * Return whether or not verification of the remote host participating
     * in data connections is enabled.  The default behavior is for
     * verification to be enabled.
     * <p>
     * @return True if verification is enabled, false if not.
     ***/
    public boolean isRemoteVerificationEnabled()
    {
        return __remoteVerificationEnabled;
    }

    /***
     * Login to the FTP server using the provided username and password.
     * <p>
     * @param username The username to login under.
     * @param password The password to use.
     * @return True if successfully completed, false if not.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending a
     *      command to the server or receiving a reply from the server.
     ***/
    public boolean login(String username, String password) throws IOException
    {
        user(username);

        if (FTPReply.isPositiveCompletion(getReplyCode()))
            return true;

        // If we get here, we either have an error code, or an intermmediate
        // reply requesting password.
        if (!FTPReply.isPositiveIntermediate(getReplyCode()))
            return false;

        return FTPReply.isPositiveCompletion(pass(password));
    }

    /***
     * Logout of the FTP server by sending the QUIT command.
     * <p>
     * @return True if successfully completed, false if not.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending a
     *      command to the server or receiving a reply from the server.
     ***/
    public boolean logout() throws IOException
    {
        return FTPReply.isPositiveCompletion(quit());
    }

    // retrieve list reply for path
    public void retrieveList(String path, List<FTPFile> entries, int limit,
      FTPFileEntryParser parser)
      throws IOException,
        FtpExceptionCanNotHaveDataConnection,
        FtpExceptionUnknownForcedDataClose,
        FtpExceptionControlClosedByForcedDataClose {
      Socket socket = __openPassiveDataConnection(FTPCommand.LIST, path);

      if (socket == null)
        throw new FtpExceptionCanNotHaveDataConnection("LIST "
          + ((path == null) ? "" : path));

      BufferedReader reader =
          new BufferedReader(new InputStreamReader(socket.getInputStream()));

      // force-close data channel socket, when download limit is reached
//      boolean mandatory_close = false;

      //List entries = new LinkedList();
      int count = 0;
      String line = parser.readNextEntry(reader);
      while (line != null) {
        FTPFile ftpFile = parser.parseFTPEntry(line);
        // skip non-formatted lines
        if (ftpFile == null) {
          line = parser.readNextEntry(reader);
          continue;
        }
        entries.add(ftpFile);
        count += line.length();
        // impose download limit if limit >= 0, otherwise no limit
        // here, cut off is up to the line when total bytes is just over limit
        if (limit >= 0 && count > limit) {
//          mandatory_close = true;
          break;
        }
        line = parser.readNextEntry(reader);
      }

      //if (mandatory_close)
      // you always close here, no matter mandatory_close or not.
      // however different ftp servers respond differently, see below.
      socket.close();

      // scenarios:
      // (1) mandatory_close is false, download limit not reached
      //     no special care here
      // (2) mandatory_close is true, download limit is reached
      //     different servers have different reply codes:

      try {
        int reply = getReply();
        if (!_notBadReply(reply))
          throw new FtpExceptionUnknownForcedDataClose(getReplyString());
      } catch (FTPConnectionClosedException e) {
        // some ftp servers will close control channel if data channel socket
        // is closed by our end before all data has been read out. Check:
        // tux414.q-tam.hp.com FTP server (hp.com version whp02)
        // so must catch FTPConnectionClosedException thrown by getReply() above
        //disconnect();
        throw new FtpExceptionControlClosedByForcedDataClose(e.getMessage());
      }

    }

    // retrieve file for path
    public void retrieveFile(String path, OutputStream os, int limit)
      throws IOException,
        FtpExceptionCanNotHaveDataConnection,
        FtpExceptionUnknownForcedDataClose,
        FtpExceptionControlClosedByForcedDataClose {

      Socket socket = __openPassiveDataConnection(FTPCommand.RETR, path);

      if (socket == null)
        throw new FtpExceptionCanNotHaveDataConnection("RETR "
          + ((path == null) ? "" : path));

      InputStream input = socket.getInputStream();

      // 20040318, xing, treat everything as BINARY_FILE_TYPE for now
      // do we ever need ASCII_FILE_TYPE?
      //if (__fileType == ASCII_FILE_TYPE)
      // input = new FromNetASCIIInputStream(input);

      // fixme, should we instruct server here for binary file type?

      // force-close data channel socket
//      boolean mandatory_close = false;

      int len; int count = 0;
      byte[] buf =
        new byte[org.apache.commons.net.io.Util.DEFAULT_COPY_BUFFER_SIZE];
      while((len=input.read(buf,0,buf.length)) != -1){
        count += len;
        // impose download limit if limit >= 0, otherwise no limit
        // here, cut off is exactly of limit bytes
        if (limit >= 0 && count > limit) {
          os.write(buf,0,len-(count-limit));
 //         mandatory_close = true;
          break;
        }
        os.write(buf,0,len);
        os.flush();
      }

      //if (mandatory_close)
      // you always close here, no matter mandatory_close or not.
      // however different ftp servers respond differently, see below.
      socket.close();

      // scenarios:
      // (1) mandatory_close is false, download limit not reached
      //     no special care here
      // (2) mandatory_close is true, download limit is reached
      //     different servers have different reply codes:

      // do not need this
      //sendCommand("ABOR");

      try {
        int reply = getReply();
        if (!_notBadReply(reply))
          throw new FtpExceptionUnknownForcedDataClose(getReplyString());
      } catch (FTPConnectionClosedException e) {
        // some ftp servers will close control channel if data channel socket
        // is closed by our end before all data has been read out. Check:
        // tux414.q-tam.hp.com FTP server (hp.com version whp02)
        // so must catch FTPConnectionClosedException thrown by getReply() above
        //disconnect();
        throw new FtpExceptionControlClosedByForcedDataClose(e.getMessage());
      }

    }

    // reply check after closing data connection
    private boolean _notBadReply(int reply) {

      if (FTPReply.isPositiveCompletion(reply)) {
        // do nothing
      } else if (reply == 426) { // FTPReply.TRANSFER_ABORTED
      // some ftp servers reply 426, e.g.,
      // foggy FTP server (Version wu-2.6.2(2)
        // there is second reply witing? no!
        //getReply();
      } else if (reply == 450) { // FTPReply.FILE_ACTION_NOT_TAKEN
      // some ftp servers reply 450, e.g.,
      // ProFTPD [ftp.kernel.org]
        // there is second reply witing? no!
        //getReply();
      } else if (reply == 451) { // FTPReply.ACTION_ABORTED
      // some ftp servers reply 451, e.g.,
      // ProFTPD [ftp.kernel.org]
        // there is second reply witing? no!
        //getReply();
      } else if (reply == 451) { // FTPReply.ACTION_ABORTED
      } else {
      // what other kind of ftp server out there?
        return false;
      }

      return true;
    }

    /***
     * Sets the file type to be transferred.  This should be one of 
     * <code> FTP.ASCII_FILE_TYPE </code>, <code> FTP.IMAGE_FILE_TYPE </code>,
     * etc.  The file type only needs to be set when you want to change the
     * type.  After changing it, the new type stays in effect until you change
     * it again.  The default file type is <code> FTP.ASCII_FILE_TYPE </code>
     * if this method is never called.
     * <p>
     * @param fileType The <code> _FILE_TYPE </code> constant indcating the
     *                 type of file.
     * @return True if successfully completed, false if not.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending a
     *      command to the server or receiving a reply from the server.
     ***/
    public boolean setFileType(int fileType) throws IOException
    {
        if (FTPReply.isPositiveCompletion(type(fileType)))
        {
/*            __fileType = fileType;
            __fileFormat = FTP.NON_PRINT_TEXT_FORMAT;*/
            return true;
        }
        return false;
    }

    /***
     * Fetches the system type name from the server and returns the string.
     * This value is cached for the duration of the connection after the
     * first call to this method.  In other words, only the first time
     * that you invoke this method will it issue a SYST command to the
     * FTP server.  FTPClient will remember the value and return the
     * cached value until a call to disconnect.
     * <p>
     * @return The system type name obtained from the server.  null if the
     *       information could not be obtained.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending a
     *  command to the server or receiving a reply from the server.
     ***/
    public String getSystemName()
      throws IOException, FtpExceptionBadSystResponse
    {
      //if (syst() == FTPReply.NAME_SYSTEM_TYPE)
      // Technically, we should expect a NAME_SYSTEM_TYPE response, but
      // in practice FTP servers deviate, so we soften the condition to
      // a positive completion.
        if (__systemName == null && FTPReply.isPositiveCompletion(syst())) {
            __systemName = (getReplyStrings()[0]).substring(4);
        } else {
            throw new FtpExceptionBadSystResponse(
              "Bad response of SYST: " + getReplyString());
        }

        return __systemName;
    }

    /***
     * Sends a NOOP command to the FTP server.  This is useful for preventing
     * server timeouts.
     * <p>
     * @return True if successfully completed, false if not.
     * @exception FTPConnectionClosedException
     *      If the FTP server prematurely closes the connection as a result
     *      of the client being idle or some other reason causing the server
     *      to send FTP reply code 421.  This exception may be caught either
     *      as an IOException or independently as itself.
     * @exception IOException  If an I/O error occurs while either sending a
     *      command to the server or receiving a reply from the server.
     ***/
    public boolean sendNoOp() throws IOException
    {
        return FTPReply.isPositiveCompletion(noop());
    }

//    client.stat(path);
//    client.sendCommand("STAT");
//    client.sendCommand("STAT",path);
//    client.sendCommand("MDTM",path);
//    client.sendCommand("SIZE",path);
//    client.sendCommand("HELP","SITE");
//    client.sendCommand("SYST");
//    client.setRestartOffset(120);

}
