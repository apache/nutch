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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.hadoop.conf.Configuration;

import com.anotherbigidea.flash.interfaces.*;
import com.anotherbigidea.flash.readers.*;
import com.anotherbigidea.flash.structs.*;
import com.anotherbigidea.flash.writers.SWFActionBlockImpl;
import com.anotherbigidea.flash.writers.SWFTagTypesImpl;
import com.anotherbigidea.io.InStream;

/**
 * Parser for Flash SWF files. Loosely based on the sample in JavaSWF
 * distribution.
 * 
 * @author Andrzej Bialecki
 */
public class SWFParser implements Parser {
  public static final Log LOG = LogFactory.getLog("org.apache.nutch.parse.swf");

  private Configuration conf = null;

  public SWFParser() {}

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  public ParseResult getParse(Content content) {

    String text = null;
    Vector outlinks = new Vector();

    try {

      byte[] raw = content.getContent();

      String contentLength = content.getMetadata().get(Response.CONTENT_LENGTH);
      if (contentLength != null && raw.length != Integer.parseInt(contentLength)) {
        return new ParseStatus(ParseStatus.FAILED, ParseStatus.FAILED_TRUNCATED,
                               "Content truncated at " + raw.length +
                               " bytes. Parser can't handle incomplete files.").getEmptyParseResult(content.getUrl(), getConf());
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
      if (atext != null && atext.length() > 0) text += "\n--------\n" + atext;
      // harvest potential outlinks
      String[] links = extractor.getUrls();
      for (int i = 0; i < links.length; i++) {
        Outlink out = new Outlink(links[i], "");
        outlinks.add(out);
      }
      Outlink[] olinks = OutlinkExtractor.getOutlinks(text, conf);
      if (olinks != null) for (int i = 0; i < olinks.length; i++) {
        outlinks.add(olinks[i]);
      }
    } catch (Exception e) { // run time exception
      e.printStackTrace(LogUtil.getErrorStream(LOG));
      return new ParseStatus(ParseStatus.FAILED, "Can't be handled as SWF document. " + e).getEmptyParseResult(content.getUrl(), getConf());
    } 
    if (text == null) text = "";

    Outlink[] links = (Outlink[]) outlinks.toArray(new Outlink[outlinks.size()]);
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "", links,
                                        content.getMetadata());
    return ParseResult.createParseResult(content.getUrl(), new ParseImpl(text, parseData));
  }

  /**
   * Arguments are: 0. Name of input SWF file.
   */
  public static void main(String[] args) throws IOException {
    FileInputStream in = new FileInputStream(args[0]);

    byte[] buf = new byte[in.available()];
    in.read(buf);
    SWFParser parser = new SWFParser();
    ParseResult parseResult = parser.getParse(new Content("file:" + args[0], "file:" + args[0],
                                          buf, "application/x-shockwave-flash",
                                          new Metadata(),
                                          NutchConfiguration.create()));
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
  protected HashMap fontCodes = new HashMap();

  public ArrayList strings = new ArrayList();

  public HashSet actionStrings = new HashSet();

  public ArrayList urls = new ArrayList();

  public ExtractText() {
    super(null);
  }

  public String getText() {
    StringBuffer res = new StringBuffer();
    Iterator it = strings.iterator();
    while (it.hasNext()) {
      if (res.length() > 0) res.append(' ');
      res.append(it.next());
    }
    return res.toString();
  }

  public String getActionText() {
    StringBuffer res = new StringBuffer();
    Iterator it = actionStrings.iterator();
    while (it.hasNext()) {
      if (res.length() > 0) res.append('\n');
      res.append(it.next());
    }
    return res.toString();
  }

  public String[] getUrls() {
    String[] res = new String[urls.size()];
    int i = 0;
    Iterator it = urls.iterator();
    while (it.hasNext()) {
      res[i] = (String) it.next();
      i++;
    }
    return res;
  }

  public void tagDefineFontInfo2(int arg0, String arg1, int arg2, int[] arg3, int arg4) throws IOException {
    tagDefineFontInfo(arg0, arg1, arg2, arg3);
  }

  /**
   * SWFTagTypes interface Save the Text Font character code info
   */
  public void tagDefineFontInfo(int fontId, String fontName, int flags, int[] codes) throws IOException {
    // System.out.println("-defineFontInfo id=" + fontId + ", name=" +
    // fontName);
    fontCodes.put(new Integer(fontId), codes);
  }

  // XXX too much hassle for too little return ... we cannot guess character
  // XXX codes anyway, so we just give up.
  /*
   * public SWFVectors tagDefineFont(int arg0, int arg1) throws IOException {
   *    return null;
   * }
   */

  /**
   * SWFTagTypes interface. Save the character code info.
   */
  public SWFVectors tagDefineFont2(int id, int flags, String name, int numGlyphs, int ascent, int descent, int leading,
          int[] codes, int[] advances, Rect[] bounds, int[] kernCodes1, int[] kernCodes2, int[] kernAdjustments)
          throws IOException {
    // System.out.println("-defineFontInfo id=" + id + ", name=" + name);
    fontCodes.put(new Integer(id), (codes != null) ? codes : new int[0]);

    return null;
  }

  /**
   * SWFTagTypes interface. Dump any initial text in the field.
   */
  public void tagDefineTextField(int fieldId, String fieldName, String initialText, Rect boundary, int flags,
          AlphaColor textColor, int alignment, int fontId, int fontSize, int charLimit, int leftMargin,
          int rightMargin, int indentation, int lineSpacing) throws IOException {
    if (initialText != null) {
      strings.add(initialText);
    }
  }

  /**
   * SWFTagTypes interface
   */
  public SWFText tagDefineText(int id, Rect bounds, Matrix matrix) throws IOException {
    lastBounds = curBounds;
    curBounds = bounds;
    return new TextDumper();
  }

  Rect lastBounds = null;
  Rect curBounds = null;

  /**
   * SWFTagTypes interface
   */
  public SWFText tagDefineText2(int id, Rect bounds, Matrix matrix) throws IOException {
    lastBounds = curBounds;
    curBounds = bounds;
    return new TextDumper();
  }

  public class TextDumper implements SWFText {
    protected Integer fontId;

    protected boolean firstY = true;

    public void font(int fontId, int textHeight) {
      this.fontId = new Integer(fontId);
    }

    public void setY(int y) {
      if (firstY)
        firstY = false;
      else strings.add("\n"); // Change in Y - dump a new line
    }

    /*
     * There are some issues with this method: sometimes SWF files define their
     * own font, so short of OCR we cannot guess what is the glyph code -> character
     * mapping. Additionally, some files don't use literal space character, instead
     * they adjust glyphAdvances. We don't handle it at all - in such cases the text
     * will be all glued together.
     */
    public void text(int[] glyphIndices, int[] glyphAdvances) {
      // System.out.println("-text id=" + fontId);
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
        // System.out.println("-ch[" + i + "]='" + chars[i] + "'(" +
        // (int)chars[i] + ") +" + glyphAdvances[i]);
      }
      strings.add(new String(chars));
    }

    public void color(Color color) {}

    public void setX(int x) {}

    public void done() {
      strings.add("\n");
    }
  }

  public SWFActions tagDoAction() throws IOException {
    // ActionTextWriter actions = new ActionTextWriter(new
    // PrintWriter(System.out));
    NutchSWFActions actions = new NutchSWFActions(actionStrings, urls);
    return actions;
  }

  public SWFActions tagDoInitAction(int arg0) throws IOException {
    // ActionTextWriter actions = new ActionTextWriter(new
    // PrintWriter(System.out));
    NutchSWFActions actions = new NutchSWFActions(actionStrings, urls);
    return actions;
  }

  public void tagGeneratorFont(byte[] arg0) throws IOException {
    // TODO Auto-generated method stub
    super.tagGeneratorFont(arg0);
  }

  public void tagGeneratorText(byte[] arg0) throws IOException {
    // TODO Auto-generated method stub
    super.tagGeneratorText(arg0);
  }

}

/**
 * ActionScript parser. This parser tries to extract free text embedded inside
 * the script, but without polluting it too much with names of variables,
 * methods, etc. Not ideal, but it works.
 * 
 * @author Andrzej Bialecki
 */
class NutchSWFActions extends SWFActionBlockImpl implements SWFActions {
  private HashSet strings = null;

  private ArrayList urls = null;

  String[] dict = null;

  Stack stack = null;

  public NutchSWFActions(HashSet strings, ArrayList urls) {
    this.strings = strings;
    this.urls = urls;
    stack = new SmallStack(100, strings);
  }

  public void lookupTable(String[] values) throws IOException {
    // System.out.println("-lookupTable: " + values.length);
    for (int i = 0; i < values.length; i++) {
      if (!strings.contains(values[i])) strings.add(values[i]);
    }
    super.lookupTable(values);
    dict = values;
  }

  public void defineLocal() throws IOException {
    // System.out.println("-defineLocal");
    stack.pop();
    super.defineLocal();
  }

  public void getURL(int vars, int mode) {
  // System.out.println("-getURL: vars=" + vars + ", mode=" + mode);
  }

  public void getURL(String url, String target) throws IOException {
    // System.out.println("-getURL: url=" + url + ", target=" + target);
    stack.push(url);
    stack.push(target);
    strings.remove(url);
    strings.remove(target);
    urls.add(url);
    super.getURL(url, target);
  }

  public SWFActionBlock.TryCatchFinally _try(String var) throws IOException {
    // System.out.println("_try: var=" + var);
    // stack.push(var);
    strings.remove(var);
    return super._try(var);
  }

  public void comment(String var) throws IOException {
    // System.out.println("-comment: var=" + var);
    // stack.push(var);
    strings.remove(var);
    super.comment(var);
  }

  public void goToFrame(String var) throws IOException {
    // System.out.println("-goToFrame: var=" + var);
    stack.push(var);
    strings.remove(var);
    super.gotoFrame(var);
  }

  public void ifJump(String var) throws IOException {
    // System.out.println("-ifJump: var=" + var);
    strings.remove(var);
    super.ifJump(var);
  }

  public void jump(String var) throws IOException {
    // System.out.println("-jump: var=" + var);
    strings.remove(var);
    super.jump(var);
  }

  public void jumpLabel(String var) throws IOException {
    // System.out.println("-jumpLabel: var=" + var);
    strings.remove(var);
    super.jumpLabel(var);
  }

  public void lookup(int var) throws IOException {
    // System.out.println("-lookup: var=" + var);
    if (dict != null && var >= 0 && var < dict.length) {
      // System.out.println(" push " + dict[var]);
      stack.push(dict[var]);
    }
    super.lookup(var);
  }

  public void push(String var) throws IOException {
    // System.out.println("-push: var=" + var);
    stack.push(var);
    strings.remove(var);
    super.push(var);
  }

  public void setTarget(String var) throws IOException {
    // System.out.println("-setTarget: var=" + var);
    stack.push(var);
    strings.remove(var);
    super.setTarget(var);
  }

  public SWFActionBlock startFunction(String var, String[] params) throws IOException {
    // System.out.println("-startFunction1: var=" + var);
    stack.push(var);
    strings.remove(var);
    if (params != null) {
      for (int i = 0; i < params.length; i++) {
        strings.remove(params[i]);
      }
    }
    return this;
  }

  public SWFActionBlock startFunction2(String var, int arg1, int arg2, String[] params, int[] arg3) throws IOException {
    // System.out.println("-startFunction2: var=" + var);
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
    // System.out.println("-waitForFrame: var=" + var);
    stack.push(var);
    strings.remove(var);
    super.waitForFrame(num, var);
  }

  public void waitForFrame(String var) throws IOException {
    // System.out.println("-waitForFrame: var=" + var);
    stack.push(var);
    strings.remove(var);
    super.waitForFrame(var);
  }

  public void done() throws IOException {
    // System.out.println("-done");
    while (stack.size() > 0) {
      strings.remove(stack.pop());
    }
  }

  public SWFActionBlock start(int arg0, int arg1) throws IOException {
    // System.out.println("-start");
    return this;
  }

  public SWFActionBlock start(int arg0) throws IOException {
    // System.out.println("-start");
    return this;
  }

  public void add() throws IOException {
    // System.out.println("-add");
    super.add();
  }

  public void asciiToChar() throws IOException {
    // System.out.println("-asciitochar");
    super.asciiToChar();
  }

  public void asciiToCharMB() throws IOException {
    // System.out.println("-asciitocharMB");
    super.asciiToCharMB();
  }

  public void push(int var) throws IOException {
    // System.out.println("-push(int)");
    if (dict != null && var >= 0 && var < dict.length) {
      // System.out.println(" push " + dict[var]);
      stack.push(dict[var]);
    }
    super.push(var);
  }

  public void callFunction() throws IOException {
    // System.out.println("-callFunction");
    strings.remove(stack.pop());
    super.callFunction();
  }

  public void callMethod() throws IOException {
    // System.out.println("-callMethod");
    strings.remove(stack.pop());
    super.callMethod();
  }

  public void getMember() throws IOException {
    // System.out.println("-getMember");
    // 0: name
    String val = (String) stack.pop();
    strings.remove(val);
    super.getMember();
  }

  public void setMember() throws IOException {
    // 0: value -1: name
    String val = (String) stack.pop();
    String name = (String) stack.pop();
    // System.out.println("-setMember: name=" + name + ", val=" + val);
    strings.remove(name);
    super.setMember();
  }

  public void setProperty() throws IOException {
    // System.out.println("-setProperty");
    super.setProperty();
  }

  public void setVariable() throws IOException {
    // System.out.println("-setVariable");
    super.setVariable();
  }

  public void call() throws IOException {
    // System.out.println("-call");
    strings.remove(stack.pop());
    super.call();
  }

  public void setTarget() throws IOException {
    // System.out.println("-setTarget");
    strings.remove(stack.pop());
    super.setTarget();
  }

  public void pop() throws IOException {
    // System.out.println("-pop");
    strings.remove(stack.pop());
    super.pop();
  }

  public void push(boolean arg0) throws IOException {
    // System.out.println("-push(b)");
    stack.push("" + arg0);
    super.push(arg0);
  }

  public void push(double arg0) throws IOException {
    // System.out.println("-push(d)");
    stack.push("" + arg0);
    super.push(arg0);
  }

  public void push(float arg0) throws IOException {
    // System.out.println("-push(f)");
    stack.push("" + arg0);
    super.push(arg0);
  }

  public void pushNull() throws IOException {
    // System.out.println("-push(null)");
    stack.push("");
    super.pushNull();
  }

  public void pushRegister(int arg0) throws IOException {
    // System.out.println("-push(reg)");
    stack.push("" + arg0);
    super.pushRegister(arg0);
  }

  public void pushUndefined() throws IOException {
    // System.out.println("-push(undef)");
    stack.push("???");
    super.pushUndefined();
  }

  public void getProperty() throws IOException {
    // System.out.println("-getProperty");
    stack.pop();
    super.getProperty();
  }

  public void getVariable() throws IOException {
    // System.out.println("-getVariable");
    strings.remove(stack.pop());
    super.getVariable();
  }

  public void gotoFrame(boolean arg0) throws IOException {
    // System.out.println("-gotoFrame(b)");
    stack.push("" + arg0);
    super.gotoFrame(arg0);
  }

  public void gotoFrame(int arg0) throws IOException {
    // System.out.println("-gotoFrame(int)");
    stack.push("" + arg0);
    super.gotoFrame(arg0);
  }

  public void gotoFrame(String arg0) throws IOException {
    // System.out.println("-gotoFrame(string)");
    stack.push("" + arg0);
    strings.remove(arg0);
    super.gotoFrame(arg0);
  }

  public void newObject() throws IOException {
    // System.out.println("-newObject");
    stack.pop();
    super.newObject();
  }

  public SWFActionBlock startWith() throws IOException {
    // System.out.println("-startWith");
    return this;
  }

}

/*
 * Small bottom-less stack.
 */
class SmallStack extends Stack {

  private int maxSize;

  private HashSet strings = null;

  public SmallStack(int maxSize, HashSet strings) {
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
    else return super.pop();
  }
}
