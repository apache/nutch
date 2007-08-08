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
package org.apache.nutch.analysis;

// JDK imports
import java.io.Reader;
import java.io.IOException;

// Lucene imports
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.hadoop.conf.Configuration;

/**
 * The analyzer used for Nutch documents. Uses the JavaCC-defined lexical
 * analyzer {@link NutchDocumentTokenizer}, with no stop list. This keeps it
 * consistent with query parsing.
 */
public class NutchDocumentAnalyzer extends NutchAnalyzer {

  /** Analyzer used to index textual content. */
  private static Analyzer CONTENT_ANALYZER;
  // Anchor Analysis
  // Like content analysis, but leave gap between anchors to inhibit
  // cross-anchor phrase matching.
  /**
   * The number of unused term positions between anchors in the anchor field.
   */
  public static final int INTER_ANCHOR_GAP = 4;
  /** Analyzer used to analyze anchors. */
  private static Analyzer ANCHOR_ANALYZER;

  /**
   * @param conf
   */
  public NutchDocumentAnalyzer(Configuration conf) {
    this.conf = conf;
    CONTENT_ANALYZER = new ContentAnalyzer(conf);
    ANCHOR_ANALYZER = new AnchorAnalyzer();
  }

  /** Analyzer used to index textual content. */
  private static class ContentAnalyzer extends Analyzer {
    private CommonGrams commonGrams;

    public ContentAnalyzer(Configuration conf) {
      this.commonGrams = new CommonGrams(conf);
    }

    /** Constructs a {@link NutchDocumentTokenizer}. */
    public TokenStream tokenStream(String field, Reader reader) {
      return this.commonGrams.getFilter(new NutchDocumentTokenizer(reader),
          field);
    }
  }

  private static class AnchorFilter extends TokenFilter {
    private boolean first = true;

    public AnchorFilter(TokenStream input) {
      super(input);
    }

    public final Token next() throws IOException {
      Token result = input.next();
      if (result == null)
        return result;
      if (first) {
        result.setPositionIncrement(INTER_ANCHOR_GAP);
        first = false;
      }
      return result;
    }
  }

  private static class AnchorAnalyzer extends Analyzer {
    public final TokenStream tokenStream(String fieldName, Reader reader) {
      return new AnchorFilter(CONTENT_ANALYZER.tokenStream(fieldName, reader));
    }
  }

  /** Returns a new token stream for text from the named field. */
  public TokenStream tokenStream(String fieldName, Reader reader) {
    Analyzer analyzer;
    if ("anchor".equals(fieldName))
      analyzer = ANCHOR_ANALYZER;
    else
      analyzer = CONTENT_ANALYZER;

    return analyzer.tokenStream(fieldName, reader);
  }
}
