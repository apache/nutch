package org.apache.nutch.parsefilter.naivebayes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Classify {

  private static int uniquewords_size = 0;

  private static int numof_ir = 0;
  private static int numwords_ir = 0;
  private static HashMap<String, Integer> wordfreq_ir = null;

  private static int numof_r = 0;
  private static int numwords_r = 0;
  private static HashMap<String, Integer> wordfreq_r = null;
  private static boolean ismodel = false;

  public static HashMap<String, Integer> unflattenToHashmap(String line) {
    HashMap<String, Integer> dict = new HashMap<String, Integer>();

    String dictarray[] = line.split(",");

    for (String field : dictarray) {

      dict.put(field.split(":")[0], Integer.valueOf(field.split(":")[1]));
    }

    return dict;

  }

  public static String classify(String line) throws IOException {

    double prob_ir = 0;
    double prob_r = 0;

    String result = "1";

    String[] linearray = line.replaceAll("[^a-zA-Z ]", "").toLowerCase()
        .split(" ");

    // read the training file
    // read the line
    if (!ismodel) {
      Configuration configuration = new Configuration();
      FileSystem fs = FileSystem.get(configuration);

      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
          fs.open(new Path("naivebayes-model"))));

      uniquewords_size = Integer.valueOf(bufferedReader.readLine());
      bufferedReader.readLine();

      numof_ir = Integer.valueOf(bufferedReader.readLine());
      numwords_ir = Integer.valueOf(bufferedReader.readLine());
      wordfreq_ir = unflattenToHashmap(bufferedReader.readLine());
      bufferedReader.readLine();
      numof_r = Integer.valueOf(bufferedReader.readLine());
      numwords_r = Integer.valueOf(bufferedReader.readLine());
      wordfreq_r = unflattenToHashmap(bufferedReader.readLine());

      ismodel = true;

      bufferedReader.close();

    }

    // update probabilities

    for (String word : linearray) {
      if (wordfreq_ir.containsKey(word))
        prob_ir += Math.log(wordfreq_ir.get(word)) + 1
            - Math.log(numwords_ir + uniquewords_size);
      else
        prob_ir += 1 - Math.log(numwords_ir + uniquewords_size);

      if (wordfreq_r.containsKey(word))
        prob_r += Math.log(wordfreq_r.get(word)) + 1
            - Math.log(numwords_r + uniquewords_size);
      else
        prob_r += 1 - Math.log(numwords_r + uniquewords_size);

    }

    prob_ir += Math.log(numof_ir) - Math.log(numof_ir + numof_r);
    prob_r += Math.log(numof_r) - Math.log(numof_ir + numof_r);

    if (prob_ir > prob_r)
      result = "0";
    else
      result = "1";

    return result;
  }

}
