package org.apache.nutch.indexer.arbitrary;


import java.io.PrintStream;

public class Echo {

  private static PrintStream out = System.out;
  private String words;

  public Echo(String args[]) {
    super();
    words = String.valueOf(args[1]);
  }

  public String getText() {
    return words;
  }

  public static void main(String[] args) {
    Echo echo = new Echo(args);
    out.println(echo.getText());
  }
}
