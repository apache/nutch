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

package org.apache.nutch.parse.rtf;

import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.apache.hadoop.conf.Configuration;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import com.etranslate.tm.processing.rtf.RTFParser;

/**
 * A parser for RTF documents
 * 
 * @author Andy Hedges
 */
public class RTFParseFactory implements Parser {

  private Configuration conf;

  public Parse getParse(Content content) throws ParseException {
    byte[] raw = content.getContent();
    Reader reader = new InputStreamReader(new ByteArrayInputStream(raw));
    RTFParserDelegateImpl delegate = new RTFParserDelegateImpl();
    RTFParser rtfParser = null;
    rtfParser = RTFParser.createParser(reader);
    rtfParser.setNewLine("\n");
    rtfParser.setDelegate(delegate);

    try {
      rtfParser.parse();
    } catch (com.etranslate.tm.processing.rtf.ParseException e) {
      throw new ParseException("Exception parsing RTF document", e);
    }

    Properties metadata = new Properties();
    metadata.putAll(content.getMetadata());
    metadata.putAll(delegate.getMetaData());
    String title = metadata.getProperty("title");

    if (title != null) {
      metadata.remove(title);
    } else {
      title = "";
    }

    String text = delegate.getText();

    return new ParseImpl(text, new ParseData(title, OutlinkExtractor
        .getOutlinks(text, this.conf), metadata));
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
}
