/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parse.ext;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseException;

import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;
import org.apache.nutch.util.CommandRunner;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;

import java.util.Hashtable;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * A wrapper that invokes external command to do real parsing job.
 * 
 * @author John Xing
 */

public class ExtParser implements Parser {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.parse.ext");

  static final int BUFFER_SIZE = 4096;

  static final int TIMEOUT_DEFAULT = 30; // in seconds

  // handy map from String contentType to String[] {command, timeoutString}
  static Hashtable TYPE_PARAMS_MAP = new Hashtable();

  // set TYPE_PARAMS_MAP using plugin.xml of this plugin
  static {
    Extension[] extensions = PluginRepository.getInstance()
      .getExtensionPoint("org.apache.nutch.parse.Parser").getExtentens();

    String contentType, command, timeoutString;

    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];

      // only look for extensions defined by plugin parse-ext
      if (!extension.getDiscriptor().getPluginId().equals("parse-ext"))
        continue;

      contentType = extension.getAttribute("contentType");
      if (contentType == null || contentType.equals(""))
        continue;

      command = extension.getAttribute("command");
      if (command == null || command.equals(""))
        continue;

      timeoutString = extension.getAttribute("timeout");
      if (timeoutString == null || timeoutString.equals(""))
        timeoutString = "" + TIMEOUT_DEFAULT;

      TYPE_PARAMS_MAP.put(contentType, new String[]{command, timeoutString});
    }
  }

  public ExtParser () {}

  public Parse getParse(Content content) throws ParseException {

    String contentType = content.getContentType();

    String[] params = (String[]) TYPE_PARAMS_MAP.get(contentType);
    if (params == null)
      throw new ParseException(
        "No external command defined for contentType: " + contentType);

    String command = params[0];
    int timeout = Integer.parseInt(params[1]);

    if (LOG.isLoggable(Level.FINE))
      LOG.fine("Use "+command+ " with timeout="+timeout+"secs");

    String text = null;
    String title = null;

    try {

      byte[] raw = content.getContent();

      String contentLength =
        (String)content.getMetadata().get("Content-Length");
      if (contentLength != null
            && raw.length != Integer.parseInt(contentLength)) {
          throw new ParseException("Content truncated at "+raw.length
            +" bytes. Parser can't handle incomplete "+contentType+" file.");
      }

      ByteArrayOutputStream os = new ByteArrayOutputStream(BUFFER_SIZE);
      ByteArrayOutputStream es = new ByteArrayOutputStream(BUFFER_SIZE/4);

      CommandRunner cr = new CommandRunner();

      cr.setCommand(command+ " " +contentType);
      cr.setInputStream(new ByteArrayInputStream(raw));
      cr.setStdOutputStream(os);
      cr.setStdErrorStream(es);

      cr.setTimeout(timeout);

      cr.evaluate();

      if (cr.getExitValue() != 0)
        throw new ParseException("External command "+command
          +" failed with error: "+es.toString());

      text = os.toString();

    } catch (ParseException e) {
      throw e;
    } catch (Exception e) { // run time exception
      throw new ParseException("ExtParser failed. "+e);
    }

    if (text == null)
      text = "";

    if (title == null)
      title = "";

    // collect outlink
    Outlink[] outlinks = new Outlink[0];

    // collect meta data
    Properties metaData = new Properties();
    metaData.putAll(content.getMetadata()); // copy through

    ParseData parseData = new ParseData(title, outlinks, metaData);
    return new ParseImpl(text, parseData);
  }

}
