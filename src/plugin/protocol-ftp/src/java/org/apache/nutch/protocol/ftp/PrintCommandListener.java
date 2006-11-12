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
import java.io.StringReader;
import java.io.IOException;

import org.apache.commons.logging.Log;

import org.apache.commons.net.ProtocolCommandEvent;
import org.apache.commons.net.ProtocolCommandListener;

/***
 * This is a support class for logging all ftp command/reply traffic.
 *
 * @author John Xing
 ***/
public class PrintCommandListener implements ProtocolCommandListener
{
    private Log __logger;

    public PrintCommandListener(Log logger)
    {
        __logger = logger;
    }

    public void protocolCommandSent(ProtocolCommandEvent event) {
      try {
        __logIt(event);
      } catch (IOException e) {
        if (__logger.isInfoEnabled()) {
          __logger.info("PrintCommandListener.protocolCommandSent(): "+e);
        }
      }
    }

    public void protocolReplyReceived(ProtocolCommandEvent event) {
      try {
        __logIt(event);
      } catch (IOException e) {
        if (__logger.isInfoEnabled()) {
          __logger.info("PrintCommandListener.protocolReplyReceived(): "+e);
        }
      }
    }

    private void __logIt(ProtocolCommandEvent event) throws IOException {
      if (!__logger.isInfoEnabled()) { return; }
      BufferedReader br =
        new BufferedReader(new StringReader(event.getMessage()));
      String line;
      while ((line = br.readLine()) != null) {
        __logger.info("ftp> "+line);
      }
    }
}
