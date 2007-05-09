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
package org.apache.nutch.parse.msexcel;

// Nutch imports
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ms.MSBaseParser;
import org.apache.nutch.protocol.Content;


/**
 * An Excel document parser.
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 * @author J&eacute;r&ocirc;me Charron
 */
public class MSExcelParser extends MSBaseParser {
    
  /**
   * Associated Mime type for Excel files
   * (<code>application/vnd.ms-excel</code>).
   */
  public static final String MIME_TYPE = "application/vnd.ms-excel";

  
  public ParseResult getParse(Content content) {
    return getParse(new ExcelExtractor(), content);
  }

  /**
   * Main for testing. Pass an excel document as argument
   */
  public static void main(String args[]) {
    main(MIME_TYPE, new MSExcelParser(), args);
  }
  
}
