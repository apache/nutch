package org.apache.nutch.indexer.arbitrary;

import java.io.PrintStream;

public class Multiplier {
  private float product = 1;
  private static PrintStream err = System.err;
  private static PrintStream out = System.out;
  
  public Multiplier(String args[]) {
    super();
  }
  
  public String getProduct(String args[]) {
    int i = args.length - 1;
    try {
      while (i >= 0) {
        product = product * Float.parseFloat(args[i]);
        i--;
      }
    } catch (NumberFormatException nfe) {
      err.println("NumberFormatException while trying to parse " + String.valueOf(args[i]));
    }
    return String.valueOf(product);
  }

  public static void main(String[] args) {
	Multiplier mp = new Multiplier(args);
	out.println(mp.getProduct(args));
  }
}
