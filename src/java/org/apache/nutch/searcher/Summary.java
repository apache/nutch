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

package org.apache.nutch.searcher;

import java.util.ArrayList;
import org.apache.nutch.html.Entities;

/** A document summary dynamically generated to match a query. */
public class Summary {

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

    /** Returns an HTML representation of this fragment. */
    public String toString() { return Entities.encode(text); }
  }

  /** A highlighted fragment of text within a summary. */
  public static class Highlight extends Fragment {
    /** Constructs a highlighted fragment for the given text. */
    public Highlight(String text) { super(text); }

    /** Returns true. */
    public boolean isHighlight() { return true; }

    /** Returns an HTML representation of this fragment. */
    public String toString() { return "<b>" + super.toString() + "</b>"; }
  }

  /** An ellipsis fragment within a summary. */
  public static class Ellipsis extends Fragment {
    /** Constructs an ellipsis fragment for the given text. */
    public Ellipsis() { super(" ... "); }

    /** Returns true. */
    public boolean isEllipsis() { return true; }

    /** Returns an HTML representation of this fragment. */
    public String toString() { return "<b> ... </b>"; }
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

  /** Returns an HTML representation of this fragment. */
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < fragments.size(); i++) {
      buffer.append(fragments.get(i));
    }
    return buffer.toString();
  }
}
