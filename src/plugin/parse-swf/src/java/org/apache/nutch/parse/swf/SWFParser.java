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

package org.apache.nutch.parse.swf;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Stack;
import java.util.Vector;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.hadoop.conf.Configuration;

import com.anotherbigidea.flash.interfaces.SWFActionBlock;
import com.anotherbigidea.flash.interfaces.SWFActions;
import com.anotherbigidea.flash.interfaces.SWFText;
import com.anotherbigidea.flash.interfaces.SWFVectors;
import com.anotherbigidea.flash.readers.SWFReader;
import com.anotherbigidea.flash.readers.TagParser;
import com.anotherbigidea.flash.structs.AlphaColor;
import com.anotherbigidea.flash.structs.Color;
import com.anotherbigidea.flash.structs.Matrix;
import com.anotherbigidea.flash.structs.Rect;
import com.anotherbigidea.flash.writers.SWFActionBlockImpl;
import com.anotherbigidea.flash.writers.SWFTagTypesImpl;
import com.anotherbigidea.io.InStream;

/**
 * Parser for Flash SWF files. Loosely based on the sample in JavaSWF
 * distribution.
 */
public class SWFParser implements Parser {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf = null;

  public SWFParser() {
    //default constructor
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public ParseResult getParse(Content content) {

    String text = null;
    Vector<Outlink> outlinks = new Vector<>();

    try {

      byte[] raw = content.getContent();

      String contentLength = content.getMetadata().get(Response.CONTENT_LENGTH);
      if (contentLength != null
          && raw.length != Integer.parseInt(contentLength)) {
        return new ParseStatus(ParseStatus.FAILED,
            ParseStatus.FAILED_TRUNCATED, "Content truncated at " + raw.length
                + " bytes. Parser can't handle incomplete files.")
            .getEmptyParseResult(content.getUrl(), getConf());
      }
      ExtractText extractor = new ExtractText();

      // TagParser implements SWFTags and drives a SWFTagTypes interface
      TagParser parser = new TagParser(extractor);
      // use this instead to debug the file
      // TagParser parser = new TagParser( new SWFTagDumper(true, true) );

      // SWFReader reads an input file and drives a SWFTags interface
      SWFReader reader = new SWFReader(parser, new InStream(raw));

      // read the input SWF file and pass it through the interface pipeline
      reader.readFile();
      text = extractor.getText();
      String atext = extractor.getActionText();
      if (atext != null && atext.length() > 0)
        text += "\n--------\n" + atext;
      // harvest potential outlinks
      String[] links = extractor.getUrls();
      for (int i = 0; i < links.length; i++) {
        Outlink out = new Outlink(links[i], "");
        outlinks.add(out);
      }
      Outlink[] olinks = OutlinkExtractor.getOutlinks(text, conf);
      if (olinks != null)
        for (int i = 0; i < olinks.length; i++) {
          outlinks.add(olinks[i]);
        }
    } catch (Exception e) { // run time exception
      LOG.error("Error, runtime exception: ", e);
      return new ParseStatus(ParseStatus.FAILED,
          "Can't be handled as SWF document. " + e).getEmptyParseResult(
          content.getUrl(), getConf());
    }
    if (text == null)
      text = "";

    Outlink[] links = (Outlink[]) outlinks
        .toArray(new Outlink[outlinks.size()]);
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "", links,
        content.getMetadata());
    return ParseResult.createParseResult(content.getUrl(), new ParseImpl(text,
        parseData));
  }

  /**
   * Arguments are: 0. Name of input SWF file.
   */
  public static void main(String[] args) throws IOException {
    FileInputStream in = new FileInputStream(args[0]);

    byte[] buf = new byte[in.available()];
    in.read(buf);
    in.close();
    SWFParser parser = new SWFParser();
    ParseResult parseResult = parser.getParse(new Content("file:" + args[0],
        "file:" + args[0], buf, "application/x-shockwave-flash",
        new Metadata(), NutchConfiguration.create()));
    Parse p = parseResult.get("file:" + args[0]);
    System.out.println("Parse Text:");
    System.out.println(p.getText());
    System.out.println("Parse Data:");
    System.out.println(p.getData());
  }
}

/**
 * Shows how to parse a Flash movie and extract all the text in Text symbols and
 * the initial text in Edit Fields. Output is to System.out.
 * 
 * A "pipeline" is set up in the main method:
 * 
 * SWFReader-->TagParser-->ExtractText
 * 
 * SWFReader reads the input SWF file and separates out the header and the tags.
 * The separated contents are passed to TagParser which parses out the
 * individual tag types and passes them to ExtractText.
 * 
 * ExtractText extends SWFTagTypesImpl and overrides some methods.
 */
class ExtractText extends SWFTagTypesImpl {
  /**
   * Store font info keyed by the font symbol id. Each entry is an int[] of
   * character codes for the correspnding font glyphs (An empty array denotes a
   * System Font).
   */
  protected HashMap<Integer, int[]> fontCodes = new HashMap<>();

  public ArrayList<String> strings = new ArrayList<>();

  public HashSet<String> actionStrings = new HashSet<>();

  public ArrayList<String> urls = new ArrayList<>();

  public ExtractText() {
    super(null);
  }

  public String getText() {
    StringBuffer res = new StringBuffer();
    Iterator<String> it = strings.iterator();
    while (it.hasNext()) {
      if (res.length() > 0)
        res.append(' ');
      res.append(it.next());
    }
    return res.toString();
  }

  public String getActionText() {
    StringBuffer res = new StringBuffer();
    String[] strings = (String[]) actionStrings
        .toArray(new String[actionStrings.size()]);
    Arrays.sort(strings);
    for (int i = 0; i < strings.length; i++) {
      if (i > 0)
        res.append('\n');
      res.append(strings[i]);
    }
    return res.toString();
  }

  public String[] getUrls() {
    String[] res = new String[urls.size()];
    int i = 0;
    Iterator<String> it = urls.iterator();
    while (it.hasNext()) {
      res[i] = it.next();
      i++;
    }
    return res;
  }

  public void tagDefineFontInfo2(int arg0, String arg1, int arg2, int[] arg3,
      int arg4) throws IOException {
    tagDefineFontInfo(arg0, arg1, arg2, arg3);
  }

  /**
   * SWFTagTypes interface Save the Text Font character code info
   */
  public void tagDefineFontInfo(int fontId, String fontName, int flags,
      int[] codes) throws IOException {
    // System.out.println("-defineFontInfo id=" + fontId + ", name=" +
    // fontName);
    fontCodes.put(Integer.valueOf(fontId), codes);
  }

  // XXX too much hassle for too little return ... we cannot guess character
  // XXX codes anyway, so we just give up.
  /*
   * public SWFVectors tagDefineFont(int arg0, int arg1) throws IOException {
   * return null; }
   */

  /**
   * SWFTagTypes interface. Save the character code info.
   */
  public SWFVectors tagDefineFont2(int id, int flags, String name,
      int numGlyphs, int ascent, int descent, int leading, int[] codes,
      int[] advances, Rect[] bounds, int[] kernCodes1, int[] kernCodes2,
      int[] kernAdjustments) throws IOException {
    fontCodes.put(Integer.valueOf(id), (codes != null) ? codes : new int[0]);

    return null;
  }

  /**
   * SWFTagTypes interface. Dump any initial text in the field.
   */
  public void tagDefineTextField(int fieldId, String fieldName,
      String initialText, Rect boundary, int flags, AlphaColor textColor,
      int alignment, int fontId, int fontSize, int charLimit, int leftMargin,
      int rightMargin, int indentation, int lineSpacing) throws IOException {
    if (initialText != null) {
      strings.add(initialText);
    }
  }

  /**
   * SWFTagTypes interface
   */
  public SWFText tagDefineText(int id, Rect bounds, Matrix matrix)
      throws IOException {
    lastBounds = curBounds;
    curBounds = bounds;
    return new TextDumper();
  }

  Rect lastBounds = null;
  Rect curBounds = null;

  /**
   * SWFTagTypes interface
   */
  public SWFText tagDefineText2(int id, Rect bounds, Matrix matrix)
      throws IOException {
    lastBounds = curBounds;
    curBounds = bounds;
    return new TextDumper();
  }

  public class TextDumper implements SWFText {
    protected Integer fontId;

    protected boolean firstY = true;

    @Override
    public void font(int fontId, int textHeight) {
      this.fontId = fontId;
    }

    @Override
    public void setY(int y) {
      if (firstY)
        firstY = false;
      else
        strings.add("\n"); // Change in Y - dump a new line
    }

    /*
     * There are some issues with this method: sometimes SWF files define their
     * own font, so short of OCR we cannot guess what is the glyph code ->
     * character mapping. Additionally, some files don't use literal space
     * character, instead they adjust glyphAdvances. We don't handle it at all -
     * in such cases the text will be all glued together.
     */
    @Override
    public void text(int[] glyphIndices, int[] glyphAdvances) {
      int[] codes = (int[]) fontCodes.get(fontId);
      if (codes == null) {
        // unknown font, better not guess
        strings.add("\n**** ?????????????? ****\n");
        return;
      }

      // --Translate the glyph indices to character codes
      char[] chars = new char[glyphIndices.length];

      for (int i = 0; i < chars.length; i++) {
        int index = glyphIndices[i];

        if (index >= codes.length) // System Font ?
        {
          chars[i] = (char) index;
        } else {
          chars[i] = (char) (codes[index]);
        }
      }
      strings.add(new String(chars));
    }

    @Override
    public void color(Color color) {
    }

    @Override
    public void setX(int x) {
    }

    @Override
    public void done() {
      strings.add("\n");
    }
  }

  @Override
  public SWFActions tagDoAction() throws IOException {
    return new NutchSWFActions(actionStrings, urls);
  }

  @Override
  public SWFActions tagDoInitAction(int arg0) throws IOException {
    return new NutchSWFActions(actionStrings, urls);
  }

}

/**
 * ActionScript parser. This parser tries to extract free text embedded inside
 * the script, but without polluting it too much with names of variables,
 * methods, etc. Not ideal, but it works.
 */
class NutchSWFActions extends SWFActionBlockImpl implements SWFActions {
  private HashSet<String> strings = null;

  private ArrayList<String> urls = null;

  String[] dict = null;

  Stack<Object> stack = null;

  public NutchSWFActions(HashSet<String> strings, ArrayList<String> urls) {
    this.strings = strings;
    this.urls = urls;
    stack = new SmallStack(100, strings);
  }

  @Override
  public void lookupTable(String[] values) throws IOException {
    for (int i = 0; i < values.length; i++) {
      if (!strings.contains(values[i]))
        strings.add(values[i]);
    }
    super.lookupTable(values);
    dict = values;
  }

  @Override
  public void defineLocal() throws IOException {
    stack.pop();
    super.defineLocal();
  }

  public void getURL(int vars, int mode) {
  }

  @Override
  public void getURL(String url, String target) throws IOException {
    stack.push(url);
    stack.push(target);
    strings.remove(url);
    strings.remove(target);
    urls.add(url);
    super.getURL(url, target);
  }

  public SWFActionBlock.TryCatchFinally _try(String var) throws IOException {
    strings.remove(var);
    return super._try(var);
  }

  @Override
  public void comment(String var) throws IOException {
    strings.remove(var);
    super.comment(var);
  }

  public void goToFrame(String var) throws IOException {
    stack.push(var);
    strings.remove(var);
    super.gotoFrame(var);
  }

  public void ifJump(String var) throws IOException {
    strings.remove(var);
    super.ifJump(var);
  }

  public void jump(String var) throws IOException {
    strings.remove(var);
    super.jump(var);
  }

  public void jumpLabel(String var) throws IOException {
    strings.remove(var);
    super.jumpLabel(var);
  }

  public void lookup(int var) throws IOException {
    if (dict != null && var >= 0 && var < dict.length) {
      stack.push(dict[var]);
    }
    super.lookup(var);
  }

  public void push(String var) throws IOException {
    stack.push(var);
    strings.remove(var);
    super.push(var);
  }

  public void setTarget(String var) throws IOException {
    stack.push(var);
    strings.remove(var);
    super.setTarget(var);
  }

  public SWFActionBlock startFunction(String var, String[] params)
      throws IOException {
    stack.push(var);
    strings.remove(var);
    if (params != null) {
      for (int i = 0; i < params.length; i++) {
        strings.remove(params[i]);
      }
    }
    return this;
  }

  public SWFActionBlock startFunction2(String var, int arg1, int arg2,
      String[] params, int[] arg3) throws IOException {
    stack.push(var);
    strings.remove(var);
    if (params != null) {
      for (int i = 0; i < params.length; i++) {
        strings.remove(params[i]);
      }
    }
    return this;
  }

  public void waitForFrame(int num, String var) throws IOException {
    stack.push(var);
    strings.remove(var);
    super.waitForFrame(num, var);
  }

  public void waitForFrame(String var) throws IOException {
    stack.push(var);
    strings.remove(var);
    super.waitForFrame(var);
  }

  public void done() throws IOException {
    while (stack.size() > 0) {
      strings.remove(stack.pop());
    }
  }

  public SWFActionBlock start(int arg0, int arg1) throws IOException {
    return this;
  }

  public SWFActionBlock start(int arg0) throws IOException {
    return this;
  }

  public void add() throws IOException {
    super.add();
  }

  public void asciiToChar() throws IOException {
    super.asciiToChar();
  }

  public void asciiToCharMB() throws IOException {
    super.asciiToCharMB();
  }

  public void push(int var) throws IOException {
    if (dict != null && var >= 0 && var < dict.length) {
      stack.push(dict[var]);
    }
    super.push(var);
  }

  public void callFunction() throws IOException {
    strings.remove(stack.pop());
    super.callFunction();
  }

  public void callMethod() throws IOException {
    strings.remove(stack.pop());
    super.callMethod();
  }

  public void getMember() throws IOException {
    // 0: name
    String val = (String) stack.pop();
    strings.remove(val);
    super.getMember();
  }

  public void setMember() throws IOException {
    // 0: value -1: name
    stack.pop(); // value
    String name = (String) stack.pop();
    strings.remove(name);
    super.setMember();
  }

  public void setProperty() throws IOException {
    super.setProperty();
  }

  public void setVariable() throws IOException {
    super.setVariable();
  }

  public void call() throws IOException {
    strings.remove(stack.pop());
    super.call();
  }

  public void setTarget() throws IOException {
    strings.remove(stack.pop());
    super.setTarget();
  }

  public void pop() throws IOException {
    strings.remove(stack.pop());
    super.pop();
  }

  public void push(boolean arg0) throws IOException {
    stack.push("" + arg0);
    super.push(arg0);
  }

  public void push(double arg0) throws IOException {
    stack.push("" + arg0);
    super.push(arg0);
  }

  public void push(float arg0) throws IOException {
    stack.push("" + arg0);
    super.push(arg0);
  }

  public void pushNull() throws IOException {
    stack.push("");
    super.pushNull();
  }

  public void pushRegister(int arg0) throws IOException {
    stack.push("" + arg0);
    super.pushRegister(arg0);
  }

  public void pushUndefined() throws IOException {
    stack.push("???");
    super.pushUndefined();
  }

  public void getProperty() throws IOException {
    stack.pop();
    super.getProperty();
  }

  public void getVariable() throws IOException {
    strings.remove(stack.pop());
    super.getVariable();
  }

  public void gotoFrame(boolean arg0) throws IOException {
    stack.push("" + arg0);
    super.gotoFrame(arg0);
  }

  public void gotoFrame(int arg0) throws IOException {
    stack.push("" + arg0);
    super.gotoFrame(arg0);
  }

  public void gotoFrame(String arg0) throws IOException {
    stack.push("" + arg0);
    strings.remove(arg0);
    super.gotoFrame(arg0);
  }

  public void newObject() throws IOException {
    stack.pop();
    super.newObject();
  }

  public SWFActionBlock startWith() throws IOException {
    return this;
  }

}

/*
 * Small bottom-less stack.
 */
class SmallStack extends Stack<Object> {

  private static final long serialVersionUID = 1L;

  private int maxSize;

  private HashSet<String> strings = null;

  public SmallStack(int maxSize, HashSet<String> strings) {
    this.maxSize = maxSize;
    this.strings = strings;
  }

  public Object push(Object o) {
    // limit max size
    if (this.size() > maxSize) {
      String val = (String) remove(0);
      strings.remove(val);
    }
    return super.push(o);
  }

  public Object pop() {
    // tolerate underruns
    if (this.size() == 0)
      return null;
    else
      return super.pop();
  }
}
