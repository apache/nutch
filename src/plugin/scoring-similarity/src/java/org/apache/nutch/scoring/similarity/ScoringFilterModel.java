package org.apache.nutch.scoring.similarity;

/**
 * This interface defines the methods to be implemented to 
 * create a plugable model in the scoring-similarity based filter 
 * @author Sujen Shah
 *
 */
public interface ScoringFilterModel {

  public int score();
}
