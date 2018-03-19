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
package org.apache.nutch.anthelion.models;

import org.apache.nutch.anthelion.classifier.RandomBinaryClassifier;

import moa.classifiers.Classifier;
import moa.classifiers.bayes.NaiveBayes;
import moa.classifiers.trees.AdaHoeffdingOptionTree;
import moa.classifiers.trees.DecisionStump;
import moa.classifiers.trees.HoeffdingAdaptiveTree;
import moa.classifiers.trees.HoeffdingTree;

/**
 * Enumeration for a selected number of classifier included in the
 * moa.classifiers package.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class ClassifierEnum {

  /**
   * Return a new classifier for the given a string
   * 
   * @param cn
   *            the name of the classifier
   * @return the {@link Classifier}
   */
  public static Classifier getClassifier(String cn) {
    Classifier classifier = null;
    switch (cn) {
    case "NaiveBayes":
      classifier = new NaiveBayes();
      break;
    case "DecisionStump":
      classifier = new DecisionStump();
      break;
    case "HoeffdingTree":
      classifier = new HoeffdingTree();
      break;
      // we removed this one because of license issues
      // case "HoeffdingTreeNG":
      // classifier = new HoeffdingTreeNG();
      // break;
    case "HoeffdingAdaptiveTree":
      classifier = new HoeffdingAdaptiveTree();
      break;
    case "AdaHoeffdingTree":
      classifier = new AdaHoeffdingOptionTree();
      break;
    case "RandomBinary":
      classifier = new RandomBinaryClassifier();
    }

    return classifier;
  }
}
