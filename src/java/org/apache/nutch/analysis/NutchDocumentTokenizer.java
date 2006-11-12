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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Token;

/** The tokenizer used for Nutch document text.  Implemented in terms of our
 * JavaCC-generated lexical analyzer, {@link NutchAnalysisTokenManager}, shared
 * with the query parser.
 */
public final class NutchDocumentTokenizer extends Tokenizer
  implements NutchAnalysisConstants {
  
  private NutchAnalysisTokenManager tokenManager;

  /** Construct a tokenizer for the text in a Reader. */
  public NutchDocumentTokenizer(Reader reader) {
    super(reader);
    tokenManager = new NutchAnalysisTokenManager(reader); 
  }
  
  /** Returns the next token in the stream, or null at EOF. */
  public final Token next() throws IOException {

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

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      System.out.print("Text: ");
      String line = in.readLine();
      Tokenizer tokenizer = new NutchDocumentTokenizer(new StringReader(line));
      Token token;
      System.out.print("Tokens: ");
      while ((token = tokenizer.next()) != null) {
        System.out.print(token.termText());
        System.out.print(" ");
      }
      System.out.println();
    }
  }

}
