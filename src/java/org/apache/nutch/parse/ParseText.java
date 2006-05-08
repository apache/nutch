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

package org.apache.nutch.parse;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.util.NutchConfiguration;

/* The text conversion of page's content, stored using gzip compression.
 * @see Parse#getText()
 */
public final class ParseText extends VersionedWritable {
  public static final String DIR_NAME = "parse_text";

  private final static byte VERSION = 1;

  public ParseText() {}
  private String text;
    
  public ParseText(String text){
    this.text = text;
  }

  public byte getVersion() { return VERSION; }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);                         // check version
    text = WritableUtils.readCompressedString(in);
    return;
  }

  public final void write(DataOutput out) throws IOException {
    super.write(out);                             // write version
    WritableUtils.writeCompressedString(out, text);
    return;
  }

  public final static ParseText read(DataInput in) throws IOException {
    ParseText parseText = new ParseText();
    parseText.readFields(in);
    return parseText;
  }

  //
  // Accessor methods
  //
  public String getText()  { return text; }

  public boolean equals(Object o) {
    if (!(o instanceof ParseText))
      return false;
    ParseText other = (ParseText)o;
    return this.text.equals(other.text);
  }

  public String toString() {
    return text;
  }

  public static void main(String argv[]) throws Exception {
    String usage = "ParseText (-local | -dfs <namenode:port>) recno segment";

    if (argv.length < 3) {
      System.out.println("usage:" + usage);
      return;
    }

    Configuration conf = NutchConfiguration.create();
    FileSystem fs = FileSystem.parseArgs(argv, 0, conf);
    try {
      int recno = Integer.parseInt(argv[0]);
      String segment = argv[1];
      String filename = new Path(segment, ParseText.DIR_NAME).toString();

      ParseText parseText = new ParseText();
      ArrayFile.Reader parseTexts = new ArrayFile.Reader(fs, filename, conf);

      parseTexts.get(recno, parseText);
      System.out.println("Retrieved " + recno + " from file " + filename);
      System.out.println(parseText);
      parseTexts.close();
    } finally {
      fs.close();
    }
  }
}
