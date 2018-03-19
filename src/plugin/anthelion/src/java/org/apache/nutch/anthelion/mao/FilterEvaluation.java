/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.anthelion.mao;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.nutch.anthelion.models.ClassificationResult;
import org.apache.nutch.anthelion.models.ClassifierEnum;

import moa.classifiers.Classifier;
import moa.core.InstancesHeader;
import moa.streams.ArffFileStream;
import moa.streams.InstanceStream;
import weka.core.Instance;

/**
 * The class uses an implementation of a stream filter, which allows the
 * reduction and manipulation of the {@link Instance}s of the underlying
 * {@link InstanceStream}. the data.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class FilterEvaluation {

  public static void main(String[] args) {
    if (args == null || args.length < 11) {
      System.out
      .println("Usage: FilterEvalution <ARFFFILE> <CLASSINDEX> <CLASSIFIER> <CLASSIFIERATTRIBUTES> <HASHSIZE> <RESULTINTERVAL> <CSVATTRIBUTESTRING> <CSVIGNOREATTRIBUTES> <CSVBINARYATTRIBUTES> <TIMESERIESATT> <TIMESERIESREPORT>");
      System.exit(0);
    } else {
      classifyStream(args[0], Integer.parseInt(args[1]),
          ClassifierEnum.getClassifier(args[2]), args[3],
          Integer.parseInt(args[4]), Integer.parseInt(args[5]),
          args[6], args[7], args[8], args[9],
          Integer.parseInt(args[10]));
    }
  }

  /**
   * Creates a list of ids (indexes) from a given set of labels within the
   * header.
   * 
   * @param header
   *            the {@link InstancesHeader}
   * @param attributeString
   *            a set of comma separated labels
   * @return a list of Integers representing the positions/indexes within the
   *         header
   */
  private static ArrayList<Integer> getAttributeIdsByName(
      InstancesHeader header, String attributeString) {
    ArrayList<Integer> ids = new ArrayList<Integer>();
    HashSet<String> attributes = new HashSet<String>(
        Arrays.asList(attributeString.split(",")));
    for (int i = 0; i < header.numAttributes(); i++) {
      if (attributes.contains(header.attribute(i).name())) {
        ids.add(i);
      }
    }
    return ids;
  }

  /**
   * Classifies data from a stream using a {@link ReduceDimensionFilter} to
   * break down large dimension vectors into a fixed smaller length. Creates a
   * new instance while processing of each input instance.
   * 
   * @param file
   *            the file (.arff) to process
   * @param classindex
   *            the index of the class in the arff file
   * @param cn
   *            the classifier to use (see {@link ClassifierEnum})
   * @param classifierOptions
   *            configuration options for the classifier as CLIString.
   * @param hashSize
   *            the size of the resulting vector which will be used for
   *            classification.
   * @param resultInterval
   *            reporting interval
   * @param csvAttributeString
   *            A comma separated string of all attribute names which should
   *            not be taken into account for the hashing
   * @param ignoreAttributes
   *            A comma separated string of all attribute names which should
   *            be ignored. Internally they will all set to 0.
   * @param makeBinaryAttributes
   *            A comma separated string of all attribute names which should
   *            be set to 0 or 1. Internally all values larger 0 will be set
   *            to 1.
   * @param timeSeriesAttribute
   *            select an attribute based on which a time series will be
   *            created for all characteristics found for this attribute. Set
   *            null if no time series should be created.
   * @param timeSeriesReportInterval
   *            set the report interval for the time series. If
   *            timeSeriesAttribute is set to null this parameter will be
   *            ignored.
   */
  private static void classifyStream(String file, int classindex,
      Classifier learner, String classifierOptions, int hashSize,
      int resultInterval, String doNotReduceAttributes,
      String ignoreAttributes, String makeBinaryAttributes,
      String timeSeriesAttribute, int timeSeriesReportInterval) {

    // create the stream to read the data
    ArffFileStream stream = new ArffFileStream(file, classindex);
    // initialize the filter
    ReduceDimensionFilter filter = new ReduceDimensionFilter();
    filter.setHashSize(hashSize);
    if (doNotReduceAttributes != null && doNotReduceAttributes.length() > 1) {
      filter.setNotHashableAttributes(getAttributeIdsByName(
          stream.getHeader(), doNotReduceAttributes));
    }
    if (ignoreAttributes != null && ignoreAttributes.length() > 1) {
      filter.setIgnorableAttributes(getAttributeIdsByName(
          stream.getHeader(), ignoreAttributes));
    }
    if (makeBinaryAttributes != null && makeBinaryAttributes.length() > 1) {
      filter.setMakeBinaryAttributes(getAttributeIdsByName(
          stream.getHeader(), makeBinaryAttributes));
    }
    filter.setInputStream(stream);

    System.out.println(learner.getPurposeString());
    InstancesHeader header = filter.getHeader();
    learner.setModelContext(header);

    // set options
    if (classifierOptions != null
        && !(classifierOptions.toLowerCase().trim().equals(""))) {
      learner.getOptions().setViaCLIString(classifierOptions);
      System.out.println("Setting learner options to "
          + classifierOptions);
    }

    learner.prepareForUse();

    // set variables
    int sampleNum = 0;
    int correctClassified = 0;
    double accuracy = 0.0;
    HashMap<String, ClassificationResult> map = new HashMap<String, ClassificationResult>();
    HashMap<String, Queue<Double>> results = new HashMap<String, Queue<Double>>();
    int attributeId = header.attribute(timeSeriesAttribute).index();

    System.out
    .println("SAMPLESIZE	ACCURACY	AVGTIMETOCLASSIFY (ms)	AVGTIMETOLEARN (ms)	AVGTIMETOLEARNLASTREPORTINGPREIOD (ms)	AVGTIMETOGETINSTANCE (ms)");
    long time = 0;
    long timeToClassify = 0;
    long timeToGetInstance = 0;
    long timeToLearn = 0;
    long learningTime = 0;
    long good = 0;
    long bad = 0;
    while (filter.hasMoreInstances()) {
      try {
        time = new Date().getTime();
        Instance instance = filter.nextInstance();
        if (instance.classValue() > 0){
          good++;
        }else{
          bad++;
        }
        timeToGetInstance += (new Date().getTime() - time);
        String val = instance.stringValue(attributeId);
        if (!map.containsKey(val)) {
          map.put(val, new ClassificationResult(val));
          results.put(val, new LinkedList<Double>());
        }
        ClassificationResult cr = map.get(val);
        cr.samplesNummer++;
        time = new Date().getTime();
        boolean result = learner.correctlyClassifies(instance);
        timeToClassify += (new Date().getTime() - time);
        if (result) {
          correctClassified++;
          cr.correctClassified++;
        }
        if (cr.samplesNummer % timeSeriesReportInterval == 0) {
          // calculate variance to current accuracy
          accuracy = (double) correctClassified / (double) sampleNum;
          double accuracyTimeSeries = (double) cr.correctClassified
              / (double) cr.samplesNummer;
          double var = Math.pow((accuracy - accuracyTimeSeries), 2);
          results.get(val).add(var);
        }
        time = new Date().getTime();
        learner.trainOnInstance(instance);
        learningTime += (new Date().getTime() - time);

        sampleNum++;
        if (sampleNum % resultInterval == 0) {

          timeToLearn += learningTime;
          accuracy = 100.0 * (double) correctClassified
              / (double) sampleNum;
          System.out.println(sampleNum + "\t" + accuracy + "\t"
              + timeToClassify / sampleNum + "\t" + timeToLearn
              / sampleNum + "\t" + learningTime / resultInterval
              + "\t" + timeToGetInstance / sampleNum);
          learningTime = 0;
        }
      } catch (Exception e) {
        System.out.println("Caugth an exception ... " + e.getMessage());
        e.printStackTrace();
      }
    }
    System.out.println(good + "/" + bad);
    DecimalFormat df = new DecimalFormat("0.0000");
    System.out.println("----------------\n\n");
    System.out.println("Variances by domain\n\n");
    for (String val : results.keySet()) {
      String toPrint = val;
      Queue<Double> q = results.get(val);
      while (!q.isEmpty()) {
        toPrint += "\t" + df.format(q.poll());
      }
      System.out.println(toPrint);
    }
  }
}
