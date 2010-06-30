package org.apache.nutch.util;

public class Pair<F, S> {

  private F first;
  private S second;

  public Pair(F first, S second) {
    super();
    this.first = first;
    this.second = second;
  }

  public F getFirst() {
    return first;
  }

  public S getSecond() {
    return second;
  }
}
