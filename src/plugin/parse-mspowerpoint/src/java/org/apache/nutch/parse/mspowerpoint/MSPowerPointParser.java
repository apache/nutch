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
package org.apache.nutch.parse.mspowerpoint;

// Nutch imports
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ms.MSBaseParser;
import org.apache.nutch.protocol.Content;


/**
 * Nutch-Parser for parsing MS PowerPoint slides ( mime type:
 * application/vnd.ms-powerpoint).
 * <p>
 * It is based on org.apache.poi.*.
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 * @author J&eacute;r&ocirc;me Charron
 * @see <a href="http://jakarta.apache.org/poi">Jakarta POI</a>
 */
public class MSPowerPointParser extends MSBaseParser {

  /**
   * Associated Mime type for PowerPoint files
   * (<code>application/vnd.ms-powerpoint</code>).
   */
  public static final String MIME_TYPE = "application/vnd.ms-powerpoint";


  public ParseResult getParse(final Content content) {
    return getParse(new PPTExtractor(), content);
  }
  
  /**
   * Main for testing. Pass a powerpoint document as argument
   */
  public static void main(String args[]) {
    main(MIME_TYPE, new MSPowerPointParser(), args);
  }
  
}
