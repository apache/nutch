package org.apache.nutch.scoring.similarity;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

public class DocumentVector {

  Map<String, Integer> termFreqVect;

  public DocumentVector(String text){
    
    termFreqVect = new HashMap<String, Integer>();
    createDocVect(text);
//    System.out.println(termFreqVect);
    removeStopWords();
//    System.out.println(termFreqVect);
  }
  
  private void createDocVect(String text){
    if(text!=null){
      String[] tokens = text.replaceAll("[^a-zA-Z0-9 ]", " ").toLowerCase().split("\\s+");
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
  
  private void removeStopWords(){
    File stopWordFile = new File("/Users/shah/Desktop/nutch/runtime/local/stopword.txt");
    try {
      String[] stopWordList1 = FileUtils.readFileToString(stopWordFile).split("\n");
      for(String stopWord: stopWordList1){
        stopWord = stopWord.trim();
        if(termFreqVect.containsKey(stopWord)){
          termFreqVect.remove(stopWord);
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
