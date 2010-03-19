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

import java.io.*;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.*;

/** The tokenizer used for Nutch document text.  Implemented in terms of our
 * JavaCC-generated lexical analyzer, {@link NutchAnalysisTokenManager}, shared
 * with the query parser.
 */
public final class NutchDocumentTokenizer extends Tokenizer
  implements NutchAnalysisConstants {

  private final NutchAnalysisTokenManager tokenManager;

  private final TermAttribute termAtt;
  private final PositionIncrementAttribute posIncrAtt;
  private final TypeAttribute typeAtt;
  private final OffsetAttribute offsetAtt;

  /** Construct a tokenizer for the text in a Reader. */
  public NutchDocumentTokenizer(Reader reader) {
    super(reader);

    tokenManager = new NutchAnalysisTokenManager(reader); 
    this.termAtt = addAttribute(TermAttribute.class);
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    this.posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    this.typeAtt = addAttribute(TypeAttribute.class);
  }

  /** Returns the next token in the stream, or null at EOF. */
  private final Token next() throws IOException {
    org.apache.nutch.analysis.Token t;

    try {
      loop: {
        while (true) {
          t = tokenManager.getNextToken();
          switch (t.kind) {                       // skip query syntax tokens
          case EOF: case WORD: case ACRONYM: case SIGRAM:
            break loop;
          default:
          }
        }
      }
    } catch (TokenMgrError e) {                   // translate exceptions
      throw new IOException("Tokenizer error:" + e);
    }

    if (t.kind == EOF)                            // translate tokens
      return null;
    else {
      return new Token(t.image,t.beginColumn,t.endColumn,tokenImage[t.kind]);
    }
  }

  /** Lucene 3.0 API. */
  public boolean incrementToken() throws IOException
  {
    clearAttributes();

    final Token t = next();
    if (t != null) {
      termAtt.setTermBuffer(t.termBuffer(), 0, t.termLength());
      offsetAtt.setOffset(t.startOffset(), t.endOffset());
      posIncrAtt.setPositionIncrement(t.getPositionIncrement());
      typeAtt.setType(t.type());
      return true;
    } else {
      return false;
    }
  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      System.out.print("Text: ");
      String line = in.readLine();
      Tokenizer tokenizer = new NutchDocumentTokenizer(new StringReader(line));
      TermAttribute termAtt = tokenizer.getAttribute(TermAttribute.class);
      System.out.print("Tokens: ");
      while (tokenizer.incrementToken()) {
        System.out.print(termAtt.term());
        System.out.print(" ");
      }
      System.out.println();
    }
  }
}
