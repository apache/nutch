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

// RTF Parser imports
import com.etranslate.tm.processing.rtf.RTFParserDelegate;

// JDK imports
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

// Nutch imports
import org.apache.nutch.metadata.DublinCore;
import org.apache.nutch.metadata.Office;


/**
 * A parser delegate for handling rtf events.
 * @author Andy Hedges
 */
public class RTFParserDelegateImpl implements RTFParserDelegate {

  String tabs = "";
  Properties metadata = new Properties();

  String[] META_NAMES_TEXT = {
    DublinCore.TITLE,
    DublinCore.SUBJECT,
    Office.AUTHOR,
    "manager",
    "company",
    "operator",
    "category",
    Office.KEYWORDS,
    Office.COMMENTS,
    "doccomm",
    "hlinkbase"
  };
  
  String[] META_NAMES_DATE = {
    "creatim",
    "creatim",
    "printim",
    "buptim"
  };

  String metaName = "";
  List metaNamesText = Arrays.asList(META_NAMES_TEXT);
  List metaNamesDate = Arrays.asList(META_NAMES_DATE);
  boolean isMetaTextValue = false;
  boolean isMetaDateValue = false;
  String content = "";
  boolean justOpenedGroup = false;
  boolean ignoreMode = false;

  public void text(String text, String style, int context) {
    justOpenedGroup = false;
    if (isMetaTextValue && context == IN_INFO) {
      metadata.setProperty(metaName, text);
      isMetaTextValue = false;
    } else if (context == IN_DOCUMENT && !ignoreMode) {
      content += text;
    }
  }

  public void controlSymbol(String controlSymbol, int context) {
    if("\\*".equals(controlSymbol) && justOpenedGroup){
      ignoreMode = true;
    }
    justOpenedGroup = false;
  }

  public void controlWord(String controlWord, int value, int context) {
    justOpenedGroup = false;
    controlWord = controlWord.substring(1);
    switch (context) {
      case IN_INFO:
        if (metaNamesText.contains(controlWord)) {
          isMetaTextValue = true;
          metaName = controlWord;
        } else if (metaNamesDate.contains(controlWord)) {
          //TODO: collect up the dates
        }
        break;
      case IN_DOCUMENT:
        //System.out.println(controlWord);
        break;
    }
  }

  public void openGroup(int depth) {
    justOpenedGroup = true;
  }

  public void closeGroup(int depth) {
    justOpenedGroup = false;
    ignoreMode = false;
  }

  public void styleList(List styles) {
  }

  public void startDocument() {
  }

  public void endDocument() {
  }

  public String getText() {
    return content;
  }

  public Properties getMetaData() {
    return metadata;
  }
}
