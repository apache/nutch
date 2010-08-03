package org.apache.nutch.parse;

import java.util.concurrent.Callable;

import org.apache.nutch.protocol.Content;

class ParseCallable implements Callable<ParseResult> {
  private Parser p;
  private Content content;
  
  public ParseCallable(Parser p, Content content) {
    this.p = p;
    this.content = content;
  }

  @Override
  public ParseResult call() throws Exception {
    return p.getParse(content);
  }    
}