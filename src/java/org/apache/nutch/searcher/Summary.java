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

package org.apache.nutch.searcher;

// JDK imports
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

// Hadoop imports
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// Nutch imports
import org.apache.nutch.html.Entities;


/** A document summary dynamically generated to match a query. */
public class Summary implements Writable {
  
  private final static int FRAGMENT  = 0;
  private final static int HIGHLIGHT = 1;
  private final static int ELLIPSIS  = 2;
  
  /** A fragment of text within a summary. */
  public static class Fragment {
    private String text;

    /** Constructs a fragment for the given text. */
    public Fragment(String text) { this.text = text; }

    /** Returns the text of this fragment. */
    public String getText() { return text; }

    /** Returns true iff this fragment is to be highlighted. */
    public boolean isHighlight() { return false; }

    /** Returns true iff this fragment is an ellipsis. */
    public boolean isEllipsis() { return false; }

    /** Returns a textual representation of this fragment. */
    public String toString() { return getText(); }

    // Inherited Javadoc
    public boolean equals(Object o) {
      try {
        Fragment f = (Fragment) o;
        return f.getText().equals(getText()) && 
               f.isHighlight() == isHighlight() &&
               f.isEllipsis() == isEllipsis();
      } catch (Exception e) {
        return false;
      }
    }
  }

  /** A highlighted fragment of text within a summary. */
  public static class Highlight extends Fragment {
    /** Constructs a highlighted fragment for the given text. */
    public Highlight(String text) { super(text); }

    /** Returns true. */
    public boolean isHighlight() { return true; }
  }

  /** An ellipsis fragment within a summary. */
  public static class Ellipsis extends Fragment {
    /** Constructs an ellipsis fragment for the given text. */
    public Ellipsis() { super(" ... "); }

    /** Returns true. */
    public boolean isEllipsis() { return true; }
  }

  private ArrayList fragments = new ArrayList();

  private static final Fragment[] FRAGMENT_PROTO = new Fragment[0];

  /** Constructs an empty Summary.*/
  public Summary() {}

  /** Adds a fragment to a summary.*/
  public void add(Fragment fragment) { fragments.add(fragment); }

  /** Returns an array of all of this summary's fragments.*/
  public Fragment[] getFragments() {
    return (Fragment[])fragments.toArray(FRAGMENT_PROTO);
  }

  /** Returns a String representation of this Summary. */
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < fragments.size(); i++) {
      buffer.append(fragments.get(i));
    }
    return buffer.toString();
  }

  /**
   * Returns a HTML representation of this Summary.
   * HTML output for <b>Highlight</b> fragments is
   * <code>&lt;span class="highlight"&gt;highlight's text&lt;/span&gt;</code>,
   * for <b>Ellipsis</b> fragments is
   * <code>&lt;span class="highlight"&gt; ... &lt;/span&gt;</code>, for generic
   * <b>Fragment</b> is simply the fragment's text.<br/>
   *
   * @param encode specifies if the summary's entities should be encoded.
   */
  public String toHtml(boolean encode) {
    Fragment fragment = null;
    StringBuffer buf = new StringBuffer();
    for (int i=0; i<fragments.size(); i++) {
      fragment = (Fragment) fragments.get(i);
      if (fragment.isHighlight()) {
        buf.append("<span class=\"highlight\">")
           .append(encode ? Entities.encode(fragment.getText())
                          : fragment.getText())
           .append("</span>");
      } else if (fragment.isEllipsis()) {
        buf.append("<span class=\"ellipsis\"> ... </span>");
      } else {
        buf.append(encode ? Entities.encode(fragment.getText())
                          : fragment.getText());
      }
    }
    return buf.toString();
  }
  
  // Inherited Javadoc
  public boolean equals(Object o) {
    if (!(o instanceof Summary)) { return false; }
    Fragment[] fragments1 = ((Summary) o).getFragments();
    Fragment[] fragments2 = getFragments();
    if (fragments1.length != fragments2.length) { return false; }
    for (int i=0; i<fragments1.length; i++) {
      if (!fragments1[i].equals(fragments2[i])) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Helper method that return a String representation for each
   * specified Summary.
   */ 
  public static String[] toStrings(Summary[] summaries) {
    if (summaries == null) { return null; }
    String[] strs = new String[summaries.length];
    for (int i=0; i<summaries.length; i++) {
      strs[i] = summaries[i].toString();
    }
    return strs;
  }

  public static Summary read(DataInput in) throws IOException {
    Summary summary = new Summary();
    summary.readFields(in);
    return summary;
  }

  
  /* ------------------------- *
   * <implementation:Writable> *
   * ------------------------- */

  // Inherited Javadoc
  public void write(DataOutput out) throws IOException {
    out.writeInt(fragments.size());
    Fragment fragment = null;
    for (int i=0; i<fragments.size(); i++) {
      fragment = (Fragment) fragments.get(i);
      if (fragment.isHighlight()) {
        out.writeByte(HIGHLIGHT);
        Text.writeString(out, fragment.getText());
      } else if (fragment.isEllipsis()) {
        out.writeByte(ELLIPSIS);
      } else {
        out.writeByte(FRAGMENT);
        Text.writeString(out, fragment.getText());
      }
    }
  }

  // Inherited Javadoc
  public void readFields(DataInput in) throws IOException {
    int nbFragments = in.readInt();
    Fragment fragment = null;
    for (int i=0; i<nbFragments; i++) {
      int type = in.readByte();
      if (type == HIGHLIGHT) {
        fragment = new Highlight(Text.readString(in));
      } else if (type == ELLIPSIS) {
        fragment = new Ellipsis();
      } else {
        fragment = new Fragment(Text.readString(in));
      }
      fragments.add(fragment);
    }
  }
  
  /* -------------------------- *
   * </implementation:Writable> *
   * -------------------------- */

}
