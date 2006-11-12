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

package org.apache.nutch.parse.mp3;

// JDK imports
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;

// Java ID3 Tag imports
import org.farng.mp3.MP3File;
import org.farng.mp3.TagException;
import org.farng.mp3.id3.AbstractID3v2;
import org.farng.mp3.id3.AbstractID3v2Frame;
import org.farng.mp3.id3.ID3v1;
import org.farng.mp3.object.AbstractMP3Object;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Nutch imports
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;


/**
 * A parser for MP3 audio files
 * @author Andy Hedges
 */
public class MP3Parser implements Parser {

  private MetadataCollector metadataCollector;
  private Configuration conf;

  public Parse getParse(Content content) {

    Parse parse = null;
    byte[] raw = content.getContent();
    File tmp = null;
    
    try {
      tmp = File.createTempFile("nutch", ".mp3");
      FileOutputStream fos = new FileOutputStream(tmp);
      fos.write(raw);
      fos.close();
      MP3File mp3 = new MP3File(tmp);

      if (mp3.hasID3v2Tag()) {
        parse = getID3v2Parse(mp3, content.getMetadata());
      } else if (mp3.hasID3v1Tag()) {
        parse = getID3v1Parse(mp3, content.getMetadata());
      } else {
        return new ParseStatus(ParseStatus.FAILED,
                               ParseStatus.FAILED_MISSING_CONTENT,
                               "No textual content available").getEmptyParse(conf);
      }
    } catch (IOException e) {
      return new ParseStatus(ParseStatus.FAILED,
                             ParseStatus.FAILED_EXCEPTION,
                             "Couldn't create temporary file:" + e).getEmptyParse(conf);
    } catch (TagException e) {
      return new ParseStatus(ParseStatus.FAILED,
                             ParseStatus.FAILED_EXCEPTION,
                             "ID3 Tags could not be parsed:" + e).getEmptyParse(conf);
    } finally{
      tmp.delete();
    }
    return parse;
  }

  private Parse getID3v1Parse(MP3File mp3, Metadata contentMeta)
  throws MalformedURLException {

    ID3v1 tag = mp3.getID3v1Tag();
    metadataCollector.notifyProperty("TALB-Text", tag.getAlbum());
    metadataCollector.notifyProperty("TPE1-Text", tag.getArtist());
    metadataCollector.notifyProperty("COMM-Text", tag.getComment());
    metadataCollector.notifyProperty("TCON-Text", "(" + tag.getGenre() + ")");
    metadataCollector.notifyProperty("TIT2-Text", tag.getTitle());
    metadataCollector.notifyProperty("TYER-Text", tag.getYear());
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
                                        metadataCollector.getTitle(),
                                        metadataCollector.getOutlinks(),
                                        contentMeta,
                                        metadataCollector.getData());
    return new ParseImpl(metadataCollector.getText(), parseData);
  }

  public Parse getID3v2Parse(MP3File mp3, Metadata contentMeta)
  throws IOException {
    
    AbstractID3v2 tag = mp3.getID3v2Tag();
    Iterator it = tag.iterator();
    while (it.hasNext()) {
      AbstractID3v2Frame frame = (AbstractID3v2Frame) it.next();
      String name = frame.getIdentifier().trim();
      if (!name.equals("APIC")) {
        Iterator itBody = frame.getBody().iterator();
        while (itBody.hasNext()) {
          AbstractMP3Object mp3Obj = (AbstractMP3Object) itBody.next();
          String bodyName = mp3Obj.getIdentifier();
          if (!bodyName.equals("Picture data")) {
            String bodyValue = mp3Obj.getValue().toString();
            metadataCollector.notifyProperty(name + "-" + bodyName, bodyValue);
          }
        }
      }
    }
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
                                        metadataCollector.getTitle(),
                                        metadataCollector.getOutlinks(),
                                        contentMeta,
                                        metadataCollector.getData());
    return new ParseImpl(metadataCollector.getText(), parseData);
  }


  public void setConf(Configuration conf) {
    this.conf = conf;
    this.metadataCollector = new MetadataCollector(conf);
  }

  public Configuration getConf() {
    return this.conf;
  }
}
