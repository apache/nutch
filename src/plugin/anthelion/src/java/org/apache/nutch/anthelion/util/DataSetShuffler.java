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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import moa.core.InstancesHeader;
import moa.streams.ArffFileStream;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;

/**
 * Util class to shuffle a dataset.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class DataSetShuffler {

  public static void main(String[] args) throws NumberFormatException,
  IOException {
    if (args == null || args.length < 3) {
      System.out
      .println("USAGE: DataSetShuffler <INPUTFILE> <CLASSINDEX> <OUTPUTFILE>");
    } else {
      shuffle(args[0], Integer.parseInt(args[1]), args[2]);
    }
  }

  public static void shuffle(String file, int classindex, String outputFile)
      throws IOException {

    // create the stream to read the data
    ArffFileStream stream = new ArffFileStream(file, classindex);
    InstancesHeader header = stream.getHeader();
    ArrayList<Instance> instanceList = new ArrayList<Instance>();
    System.out.println("Loading data ...");
    int cnt = 0;
    while (stream.hasMoreInstances()) {
      if (++cnt % 10000 == 0) {
        System.out.println("Read " + cnt + " items.");
      }
      instanceList.add(stream.nextInstance());
    }
    System.out.println("Read all items ... shuffling.");
    Collections.shuffle(instanceList);
    ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
    for (int i = 0; i < header.numAttributes(); i++) {
      attributeList.add(header.attribute(i));
    }
    Instances dataSet = new Instances("reduced", attributeList, 2);
    for (Instance inst : instanceList) {
      dataSet.add(inst);
      inst.setDataset(dataSet);
    }
    System.out.println("Writing output ...");
    ArffSaver saver = new ArffSaver();
    saver.setInstances(dataSet);
    saver.setFile(new File(outputFile));
    saver.writeBatch();
    System.out.println("Done.");
  }

}
