package org.apache.nutch.scoring;

/**
 * Specialized exception for errors during scoring.
 * 
 * @author Andrzej Bialecki
 */
public class ScoringFilterException extends Exception {

  public ScoringFilterException() {
    super();
  }

  public ScoringFilterException(String message) {
    super(message);
  }

  public ScoringFilterException(String message, Throwable cause) {
    super(message, cause);
  }

  public ScoringFilterException(Throwable cause) {
    super(cause);
  }

}
