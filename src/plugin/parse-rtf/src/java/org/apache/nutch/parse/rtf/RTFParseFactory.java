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
package org.apache.nutch.parse.rtf;

// JDK imports
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Nutch imports
import org.apache.nutch.metadata.DublinCore;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;

// RTF Parser imports
import com.etranslate.tm.processing.rtf.ParseException;
import com.etranslate.tm.processing.rtf.RTFParser;


/**
 * A parser for RTF documents
 * 
 * @author Andy Hedges
 */
public class RTFParseFactory implements Parser {

  private Configuration conf;

  public Parse getParse(Content content) {
    byte[] raw = content.getContent();
    Reader reader = new InputStreamReader(new ByteArrayInputStream(raw));
    RTFParserDelegateImpl delegate = new RTFParserDelegateImpl();
    RTFParser rtfParser = null;
    rtfParser = RTFParser.createParser(reader);
    rtfParser.setNewLine("\n");
    rtfParser.setDelegate(delegate);

    try {
      rtfParser.parse();
    } catch (ParseException e) {
        return new ParseStatus(ParseStatus.FAILED,
                               ParseStatus.FAILED_EXCEPTION,
                               e.toString()).getEmptyParse(conf);
    }

    Metadata metadata = new Metadata();
    metadata.setAll(delegate.getMetaData());
    String title = metadata.get(DublinCore.TITLE);

    if (title != null) {
      metadata.remove(DublinCore.TITLE);
    } else {
      title = "";
    }

    String text = delegate.getText();

    return new ParseImpl(text,
                         new ParseData(ParseStatus.STATUS_SUCCESS,
                                       title,
                                       OutlinkExtractor
        .                              getOutlinks(text, this.conf),
                                       content.getMetadata(),
                                       metadata));
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
}
