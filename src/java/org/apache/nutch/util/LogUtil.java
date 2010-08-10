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
package org.apache.nutch.util;

// JDK imports
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for logging.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public class LogUtil {

  private final static Logger LOG = LoggerFactory.getLogger(LogUtil.class);

  private static Method TRACE = null;
  private static Method DEBUG = null;
  private static Method INFO  = null;
  private static Method WARN  = null;
  private static Method ERROR = null;
  private static Method FATAL = null;

  static {
    try {
      TRACE = Logger.class.getMethod("trace", new Class[] { Object.class });
      DEBUG = Logger.class.getMethod("debug", new Class[] { Object.class });
      INFO  = Logger.class.getMethod("info",  new Class[] { Object.class });
      WARN  = Logger.class.getMethod("warn",  new Class[] { Object.class });
      ERROR = Logger.class.getMethod("error", new Class[] { Object.class });
      FATAL = Logger.class.getMethod("fatal", new Class[] { Object.class });
    } catch(Exception e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Cannot init log methods", e);
      }
    }
  }
  
  
  public static PrintStream getTraceStream(final Logger logger) {
    return getLogStream(logger, TRACE);
  }

  public static PrintStream getDebugStream(final Logger logger) {
    return getLogStream(logger, DEBUG);
  }

  public static PrintStream getInfoStream(final Logger logger) {
    return getLogStream(logger, INFO);
  }
  
  public static PrintStream getWarnStream(final Logger logger) {
    return getLogStream(logger, WARN);
  }

  public static PrintStream getErrorStream(final Logger logger) {
    return getLogStream(logger, ERROR);
  }

  public static PrintStream getFatalStream(final Logger logger) {
    return getLogStream(logger, FATAL);
  }
  
  /** Returns a stream that, when written to, adds log lines. */
  private static PrintStream getLogStream(final Logger logger, final Method method) {
    return new PrintStream(new ByteArrayOutputStream() {
        private int scan = 0;

        private boolean hasNewline() {
          for (; scan < count; scan++) {
            if (buf[scan] == '\n')
              return true;
          }
          return false;
        }

        public void flush() throws IOException {
          if (!hasNewline())
            return;
          try {
            method.invoke(logger, new Object[] { toString().trim() });
          } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
              LOG.error("Cannot log with method [" + method + "]", e);
            }
          }
          reset();
          scan = 0;
        }
      }, true);
  }

}
