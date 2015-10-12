package org.apache.nutch.parsefilter.naivebayes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Train {

  public static String replacefirstoccuranceof(String tomatch, String line) {

    int index = line.indexOf(tomatch);
    if (index == -1) {
      return line;
    } else {
      return line.substring(0, index)
          + line.substring(index + tomatch.length());
    }

  }

  public static void updateHashMap(HashMap<String, Integer> dict, String key) {
    if (!key.equals("")) {
      if (dict.containsKey(key))
        dict.put(key, dict.get(key) + 1);
      else
        dict.put(key, 1);
    }
  }

  public static String flattenHashMap(HashMap<String, Integer> dict) {
    String result = "";

    for (String key : dict.keySet()) {

      result += key + ":" + dict.get(key) + ",";
    }

    // remove the last comma
    result = result.substring(0, result.length() - 1);

    return result;
  }

  public static void start(String filepath) throws IOException {

    // two classes 0/irrelevant and 1/relevant

    // calculate the total number of instances/examples per class, word count in
    // each class and for each class a word:frequency map

    int numof_ir = 0;
    int numof_r = 0;
    int numwords_ir = 0;
    int numwords_r = 0;
    HashSet<String> uniquewords = new HashSet<String>();
    HashMap<String, Integer> wordfreq_ir = new HashMap<String, Integer>();
    HashMap<String, Integer> wordfreq_r = new HashMap<String, Integer>();

    String line = "";
    String target = "";
    String[] linearray = null;

    // read the line
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(configuration);

    BufferedReader bufferedReader = new BufferedReader(
        configuration.getConfResourceAsReader(filepath));

    while ((line = bufferedReader.readLine()) != null) {

      target = line.split("\t")[0];

      line = replacefirstoccuranceof(target + "\t", line);

      linearray = line.replaceAll("[^a-zA-Z ]", "").toLowerCase().split(" ");

      // update the data structures
      if (target.equals("0")) {

        numof_ir += 1;
        numwords_ir += linearray.length;
        for (int i = 0; i < linearray.length; i++) {
          uniquewords.add(linearray[i]);
          updateHashMap(wordfreq_ir, linearray[i]);
        }
      } else {

        numof_r += 1;
        numwords_r += linearray.length;
        for (int i = 0; i < linearray.length; i++) {
          uniquewords.add(linearray[i]);
          updateHashMap(wordfreq_r, linearray[i]);
        }

      }

    }

    // write the model file

    Path path = new Path("naivebayes-model");

    Writer writer = new BufferedWriter(new OutputStreamWriter(fs.create(path,
        true)));

    writer.write(String.valueOf(uniquewords.size()) + "\n");
    writer.write("0\n");
    writer.write(String.valueOf(numof_ir) + "\n");
    writer.write(String.valueOf(numwords_ir) + "\n");
    writer.write(flattenHashMap(wordfreq_ir) + "\n");
    writer.write("1\n");
    writer.write(String.valueOf(numof_r) + "\n");
    writer.write(String.valueOf(numwords_r) + "\n");
    writer.write(flattenHashMap(wordfreq_r) + "\n");

    writer.close();

    bufferedReader.close();

  }

}
