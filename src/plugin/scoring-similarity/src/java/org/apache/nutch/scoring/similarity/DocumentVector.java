package org.apache.nutch.scoring.similarity;

import java.util.HashMap;
import java.util.Map;

public class DocumentVector {

  Map<String, Integer> termFreqVect;
  
  public DocumentVector(String text){
    
    termFreqVect = new HashMap<String, Integer>();
    createDocVect(text);
  }
  
  private void createDocVect(String text){
    if(text!=null){
      String[] tokens = text.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
      for(String token: tokens){
        if(termFreqVect.containsKey(token)){
          int count = termFreqVect.get(token)+1;
          termFreqVect.put(token, count);
        }
        else
          termFreqVect.put(token, 1);
      }
    }
    else
      System.out.println("Text for creating doc is null");
    
  }
  
  public Map<String, Integer> getTermFreqVect(){
    return termFreqVect;
  }
}
