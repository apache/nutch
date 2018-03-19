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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.nutch.anthelion.models.ClassifierEnum;

import moa.classifiers.Classifier;
import moa.core.InstancesHeader;
import moa.streams.ArffFileStream;
import moa.streams.InstanceStream;
import weka.core.Instance;

/**
 * The class uses an implementation of a stream filter, which allows the
 * reduction and manipulation of the {@link Instance}s of the underlying
 * {@link InstanceStream}. The filter also shuffles the cache before processing
 * the data.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class ShuffledFilterEvaluation {

  public static void main(String[] args) {
    if (args == null || args.length < 10) {
      System.out
      .println("Usage: ShuffeledFilterEvalution <ARFFFILE> <CLASSINDEX> <CLASSIFIER> <HASHSIZE> <CACHESIZE> <CACHEREFILLRATIO> <RESULTINTERVAL> <CSVDONTREDUCEATTRIBUTES> <CSVIGNOREATTRIBUTES> <CSVBINARYATTRIBUTES>");
      System.exit(0);
    } else {
      classifyStream(args[0], Integer.parseInt(args[1]), args[2],
          Integer.parseInt(args[3]), Integer.parseInt(args[4]),
          Integer.parseInt(args[5]), Integer.parseInt(args[6]),
          args[7], args[8], args[9]);
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
   *            the index of the class in the ARFF file
   * @param cn
   *            the classifier to use (see {@link ClassifierEnum})
   * @param hashSize
   *            the size of the resulting vector which will be used for
   *            classification.
   * @param cacheSize
   *            the size of items to load into cache. Items in cache will be
   *            shuffled before processed.
   * @param cacheRatioForRefill
   *            ratio x when the cache is refilled. If cache size is lower
   *            than 1/x of cache size the cache will be reload.
   * @param resultInterval
   *            reporting interval
   * @param doNotReduceAttributes
   *            A comma separated string of all attribute names which should
   *            not be taken into account for the hashing
   * @param ignoreAttributes
   *            A comma separated string of all attribute names which should
   *            be ignored. Internally they will all set to 0.
   * @param makeBinaryAttributes
   *            A comma separated string of all attribute names which should
   *            be set to 0 or 1. Internally all values larger 0 will be set
   *            to 1.
   */
  private static void classifyStream(String file, int classindex, String cn,
      int hashSize, int cacheSize, int cacheRatioForRefill,
      int resultInterval, String doNotReduceAttributes,
      String ignoreAttributes, String makeBinaryAttributes) {

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

    Classifier learner = ClassifierEnum.getClassifier(cn);

    InstancesHeader header = filter.getHeader();
    learner.setModelContext(header);
    learner.prepareForUse();

    // set variables
    int maxItemSize = cacheSize;
    int minItemSizeRatio = cacheRatioForRefill;
    int sampleNum = 0;
    int correctClassified = 0;
    double accuracy = 0.0;

    List<Instance> arffItems = new ArrayList<Instance>();

    System.out.println("SAMPLESIZE	ACCURACY");

    boolean run = true;
    while (run) {

      while (arffItems.size() > 0
          && (arffItems.size() > ((int) maxItemSize / minItemSizeRatio) || !filter
              .hasMoreInstances())) {

        Instance instance = arffItems.remove(0);

        boolean result = learner.correctlyClassifies(instance);

        if (result) {
          correctClassified++;
        }
        learner.trainOnInstance(instance);
        sampleNum++;
        if (sampleNum % resultInterval == 0) {
          accuracy = 100.0 * (double) correctClassified
              / (double) sampleNum;
          System.out.println(sampleNum + "\t" + accuracy);
        }

      }
      while (filter.hasMoreInstances() && arffItems.size() < maxItemSize) {
        arffItems.add(filter.nextInstance());
      }
      // shake it baby!
      Collections.shuffle(arffItems);

      if (!filter.hasMoreInstances() && arffItems.size() == 0) {
        accuracy = 100.0 * (double) correctClassified
            / (double) sampleNum;
        // all items processed
        run = false;
      }
    }
  }
}
