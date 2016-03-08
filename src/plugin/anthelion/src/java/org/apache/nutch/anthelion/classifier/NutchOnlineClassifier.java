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
package org.apache.nutch.anthelion.classifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.nutch.anthelion.framework.AnthOnlineClassifier;
import org.apache.nutch.anthelion.models.AnthURL;
import org.apache.nutch.anthelion.models.ClassifierEnum;

import moa.classifiers.Classifier;
import moa.core.InstancesHeader;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;

/**
 * 
 * A wrapper-class for {@link Classifier} of the MOA library to be used in the
 * Nutch plugin. (based on the {@link AnthOnlineClassifier})
 * 
 * @author Petar Ristoski (petar@dwslab.de)
 *
 */
public class NutchOnlineClassifier {

  private static Classifier learner;
  private static Queue<Instance> learningQueue;
  private static int dimension;
  private static Instances instances;
  protected static long unclassifiableItems;
  protected static long classifiedItems;
  private static double[] replaceMissingValues;

  private ArrayList<String> boolAttValues = new ArrayList<String>(Arrays.asList("0", "1"));

  private static HashMap<String, Integer> attributesIndex;

  private static String numPattern = "[0-9]*";
  private static String stringPattern = "[a-z]*";
  private static String numReplacementString = "[NUMBER]";
  private static int hashTrickSize;
  private static int batchLearningSize;

  /**
   * Sets the prediction variable within a {@link AnthURL} based on the
   * learned classifier. As the {@link AnthURL} implements {@link Comparable}
   * Interface and is included in a {@link PriorityQueue} this effects the
   * ordering.
   * 
   * @param aurl
   *            the {@link AnthURL}
   */
  public static void classifyUrl(AnthURL aurl) {
    Instance inst = convert(aurl);
    if (inst != null) {
      // good class = 0, bad class = 1
      double[] res = learner.getVotesForInstance(inst);
      classifiedItems++;
      if (res.length < 2) {
        unclassifiableItems++;
        aurl.prediction = 0;
      } else {
        // aurl.prediction = res[0];
        aurl.prediction = res[0] - res[1];
      }
    } else {
      aurl.prediction = 0;
    }

  }

  /**
   * Internal function which initialized the {@link Instances} used by the
   * {@link Classifier} wrapped by the {@link AnthOnlineClassifier} class.
   */
  private void initInstances() {
    // gather attributes
    ArrayList<Attribute> attributes = new ArrayList<Attribute>();
    ArrayList<String> allowedClasses = new ArrayList<String>();
    allowedClasses.add("sem");
    allowedClasses.add("nonsem");
    Attribute classAttribute = new Attribute("class", allowedClasses);
    attributes.add(classAttribute);
    // this looks somehow stupid to me :/
    List<String> vector = null;
    attributes.add(new Attribute("domain", vector));
    attributes.add(new Attribute("sempar"));
    attributes.add(new Attribute("nonsempar"));
    attributes.add(new Attribute("semsib"));
    attributes.add(new Attribute("nonsemsib"));
    for (int i = 0; i < hashTrickSize; i++) {
      // the boolAttValues here should not be necessary but based on some
      // runtime experiements they make a (slight) difference as it is not
      // possible to create directly boolean attributes. The time to
      // define a split is reduced by doing this with nominal.
      attributes.add(new Attribute(getAttributeNameOfHash(i), boolAttValues));
    }
    // now we create the Instances
    instances = new Instances("Anthelion", attributes, 1);
    instances.setClass(classAttribute);
    attributesIndex = new HashMap<String, Integer>();
    for (int i = 0; i < attributes.size(); i++) {
      attributesIndex.put(attributes.get(i).name(), i);
    }
    // set dimension (class + domain + 4xgraph + hashes)
    dimension = 1 + 1 + 4 + hashTrickSize;
    // init replacement array
    replaceMissingValues = new double[dimension];
    for (int i = 0; i < dimension; i++) {
      replaceMissingValues[i] = 0.0;
    }
  }

  /**
   * Instanciates the {@link NutchOnlineClassifier} class which wrapes all
   * necessary information to do online classification (training and
   * classification itself) based on {@link AnthURL}.
   */
  @SuppressWarnings("static-access")
  public NutchOnlineClassifier(String classifierName, String classifierOptions, int hashTrickSize,
      int batchLearnSize) {
    this.hashTrickSize = hashTrickSize;
    this.batchLearningSize = batchLearnSize;
    // we create the classifier only once
    if (learner == null) {
      learningQueue = new LinkedList<Instance>();
      initInstances();
      learner = ClassifierEnum.getClassifier(classifierName);
      learner.setModelContext(new InstancesHeader(instances));
      if (classifierOptions != null && !classifierOptions.equals("")) {
        learner.getOptions().setViaCLIString(classifierOptions);
      }
      learner.prepareForUse();
    }

    // here we go!
  }

  /**
   * Converts an {@link AnthURL} into an {@link Instance} which can be handled
   * by the {@link Classifier}.
   * 
   * @param url
   *            the {@link AnthURL} which should be transformed/converted.
   * @return the resulting {@link Instance}.
   */
  private static Instance convert(AnthURL url) {
    if (url != null) {

      Instance inst = new SparseInstance(dimension);
      inst.replaceMissingValues(replaceMissingValues);

      inst.setDataset(instances);
      inst.setValue(attributesIndex.get("class"), (url.sem ? "sem" : "nonsem"));
      inst.setValue(attributesIndex.get("sempar"), (url.semFather ? 1 : 0));
      inst.setValue(attributesIndex.get("nonsempar"), (url.nonSemFather ? 1 : 0));
      inst.setValue(attributesIndex.get("semsib"), (url.semSibling ? 1 : 0));
      inst.setValue(attributesIndex.get("nonsempar"), (url.nonSemFather ? 1 : 0));
      inst.setValue(attributesIndex.get("domain"), url.uri.getHost());
      Set<String> tokens = new HashSet<String>();

      tokens.addAll(tokenizer(url.uri.getPath()));
      tokens.addAll(tokenizer(url.uri.getQuery()));
      tokens.addAll(tokenizer(url.uri.getFragment()));
      for (String tok : tokens) {
        inst.setValue(attributesIndex.get(getAttributeNameOfHash(getHash(tok, hashTrickSize))), 1);
      }
      return inst;

    } else {
      System.out.println("Input AnthURL for convertion into instance was null.");
      return null;
    }
  }

  /**
   * Pushes feedback from a crawled {@link AnthURL} into the classifier. The
   * learning is batched based in the
   * {@link RuntimeConfig#BATCH_INSTANCE_FOR_LEARNING} variable.
   * 
   * @param aurl
   *            the {@link AnthURL} including the feedback.
   */
  public static void pushFeedback(AnthURL aurl) {
    Instance inst = convert(aurl);
    learningQueue.add(inst);
    if (learningQueue.size() > batchLearningSize) {
      learn();
    }
  }

  /**
   * 
   * @param aurl
   */
  public void initialize(List<AnthURL> list) {
    System.out.println("Initializing learner with " + list.size() + " Urls.");
    for (AnthURL aurl : list) {
      Instance inst = convert(aurl);
      learningQueue.add(inst);
    }
    System.out.println("Start learning ...");
    learn();
    System.out.println("... learning done.");
  }

  /**
   * Implementation of the tokenization process based on the URL-String. A set
   * of tokens (unique) is returned.
   * 
   * @param string
   *            the String which should be tokenzied.
   * @return a set of tokens.
   */
  private static Set<String> tokenizer(String string) {
    HashSet<String> tokens = new HashSet<String>();
    if (string != null && !string.equals("")) {
      // split
      String tok[] = string.toLowerCase().split("([^a-z0-9])");
      // filter

      for (String t : tok) {
        // length > 2
        if (t.length() < 3) {
          continue;
        }
        // replace number
        if (t.matches(numPattern)) {
          tokens.add(numReplacementString);
          continue;
        }
        // filter all string pattern
        if (t.matches(stringPattern)) {
          tokens.add(t);
          continue;
        }
      }
    }
    return tokens;
  }

  /**
   * Initiates a learning process.
   */
  private static void learn() {
    int listSize = learningQueue.size();
    for (int i = 0; i < listSize; i++) {
      Instance inst = learningQueue.poll();
      if (inst != null)
        learner.trainOnInstance(inst);
    }
  }

  /**
   * Returns the name of the hash-attribute for position i
   * 
   * @param i
   *            the position of the hash
   * @return the corresponding attribute name.
   */
  private static String getAttributeNameOfHash(int i) {
    return "hash" + i;
  }

  /**
   * This is a simple functions which returns a hash within a fixed limited.
   * The returned has will be between 0 and (limit - 1)
   * 
   * @param str
   *            The {@link String} which should be hashed.
   * @param limit
   *            The maximal hash-size (int)
   * @return the corresponding hash value between 0 and (limit - 1) for the
   *         given input {@link String}
   */
  private static int getHash(String str, int limit) {
    int hashCode = Math.abs(str.hashCode() % (limit));
    return hashCode;
  }

  public static void main(String[] args) {

    try {
      URI urlTmp = new URI("http://www.google.com");
      System.out.println(urlTmp.getPath());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}
