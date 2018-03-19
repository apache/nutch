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
package org.apache.nutch.anthelion.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.nutch.anthelion.mao.ReduceDimensionFilter;
import org.apache.nutch.anthelion.models.ClassificationResult;

import moa.classifiers.Classifier;
import moa.classifiers.trees.DecisionStump;
import moa.core.InstancesHeader;
import moa.streams.ArffFileStream;
import weka.core.Instance;
import de.uni_mannheim.informatik.dws.dwslib.util.InputUtil;

/**
 * Helper to evaluate a set of input files and evaluate them one by one by
 * keeping the model in RAM.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class DomainSpecificEvaluation {

  private HashMap<String, Classifier> classifierMap = new HashMap<String, Classifier>();
  private ArrayList<ClassificationResult> resultList = new ArrayList<ClassificationResult>();

  public static void main(String[] args) throws IOException {

    int numRuns = 1;
    int steps = 5;
    int start = 1;
    int end = 101;
    System.out
    .println("RUNS	SAMPLESIZE	ACCUMULTEDSAMPLESIZE	ACCUMULAETDCORRECTSAMPLES	ACCURACY");
    for (int i = start; i < end; i++) {
      if (i % steps != 0) {
        continue;
      }
      int sampleNum = i;
      int sample = 0;
      int correct = 0;
      for (int run = 0; run < numRuns; run++) {
        List<String> inputFiles = InputUtil
            .getFileReferenceList(args[0]);

        DomainSpecificEvaluation eval = new DomainSpecificEvaluation();
        for (String file : inputFiles) {
          eval.classifyStream(
              file.replace(args[0], "").replace(".arff", ""),
              file, 2, sampleNum);
        }
        long sampleCount = 0;
        long correctClassificationCnt = 0;
        for (ClassificationResult res : eval.resultList) {
          sampleCount += res.samplesNummer;
          correctClassificationCnt += res.correctClassified;
          // System.out.println(res.toString());
        }
        sample += sampleCount;
        correct += correctClassificationCnt;
      }
      double globalAccuracy = 100.0 * (double) correct / (double) sample;
      System.out.println(numRuns + "	" + sampleNum + "	" + sample + "	"
          + correct + "	" + globalAccuracy);
    }
  }

  private static ArrayList<Integer> getStableAttributes(InstancesHeader header) {
    ArrayList<Integer> ids = new ArrayList<Integer>();
    HashSet<String> attributes = new HashSet<String>((Arrays.asList("id",
        "domain", "nonsempar", "nonsemsib", "sempar", "semsib",
        "domain", "token_length", "document_length")));
    for (int i = 0; i < header.numAttributes(); i++) {
      if (attributes.contains(header.attribute(i).name())) {
        ids.add(i);
      }
    }

    return ids;
  }

  public void classifyStream(String domain, String file, int classindex,
      int numOfSamplesToUse) {
    // Classifier learner = new NaiveBayes();
    Classifier learner = new DecisionStump();
    // Classifier learner = new moa.classifiers.trees.HoeffdingTreeNG();
    // Classifier learner = new
    // moa.classifiers.rules.RuleClassifierNBayes();
    classifierMap.put(domain, learner);
    ArffFileStream stream = new ArffFileStream(file, classindex);
    ReduceDimensionFilter filter = new ReduceDimensionFilter();

    filter.setHashSize(1000);
    filter.setNotHashableAttributes(getStableAttributes(stream.getHeader()));
    filter.setInputStream(stream);

    learner.setModelContext(filter.getHeader());
    learner.prepareForUse();
    List<Instance> arffItems = new ArrayList<Instance>();

    while (filter.hasMoreInstances()) {
      arffItems.add(filter.nextInstance());
    }
    // shake it baby!
    Collections.shuffle(arffItems);

    int sampleNum = 0;
    int correctClassified = 0;
    double accuracy = 0.0;
    for (Instance instance : arffItems) {

      if (learner.correctlyClassifies(instance)) {
        correctClassified++;
      }

      learner.trainOnInstance(instance);
      sampleNum++;
      if (!(sampleNum < numOfSamplesToUse)) {
        break;
      }

    }
    accuracy = 100.0 * (double) correctClassified / (double) sampleNum;
    resultList.add(new ClassificationResult(accuracy, sampleNum,
        correctClassified, domain));
  }

}
