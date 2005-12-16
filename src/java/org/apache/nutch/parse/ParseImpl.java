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
import org.apache.nutch.io.*;


/** The result of parsing a page's raw content.
 * @see Parser#getParse(Content)
 */
public class ParseImpl implements Parse, Writable {
  private ParseText text;
  private ParseData data;

  public ParseImpl() {}

  public ParseImpl(Parse parse) {
    this(parse.getText(), parse.getData());
  }

  public ParseImpl(String text, ParseData data) {
    this(new ParseText(text), data);
  }

  public ParseImpl(ParseText text, ParseData data) {
    this.text = text;
    this.data = data;
  }

  public String getText() { return text.getText(); }

  public ParseData getData() { return data; }
  
  public final void write(DataOutput out) throws IOException {
    text.write(out);
    data.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    text = new ParseText();
    text.readFields(in);

    data = new ParseData();
    data.readFields(in);
  }

  public static ParseImpl read(DataInput in) throws IOException {
    ParseImpl parseImpl = new ParseImpl();
    parseImpl.readFields(in);
    return parseImpl;
  }

}
