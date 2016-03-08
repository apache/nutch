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
import java.util.HashMap;

import moa.classifiers.Classifier;
import moa.cluster.Cluster;
import moa.core.InstancesHeader;
import moa.streams.filters.AbstractStreamFilter;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;

/**
 * This filter manipulates the instances, coming from an input stream and allows
 * the reduction of the number of dimensions. This is especially meaningful when
 * working with text-based features as tokens and one does not want to hold a
 * complete dictionary of all possible tokens which may arise during parsing the
 * stream in memory. This filter implements the hash-trick. In addition the
 * {@link ReduceDimensionFilter} allows to manipulate the attributes by either
 * making them binary (if attribute > 0 --> 1, 0 else) or simply ignoring
 * attributes.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class ReduceDimensionFilter extends AbstractStreamFilter {

  private static final long serialVersionUID = 1L;
  private int hashSize;

  private Instances newInstances;
  private HashMap<String, Integer> attributesIndex;
  private ArrayList<Integer> ignoreAttributes = new ArrayList<Integer>();
  private ArrayList<Integer> makeBinaryAttributes = new ArrayList<Integer>();
  private ArrayList<Integer> notHashableAttributes = new ArrayList<Integer>();
  private double[] replacementArray;

  private ArrayList<String> boolAttValues = new ArrayList<String>(
      Arrays.asList("0", "1"));

  /**
   * Set the number of hashes which should be used in the final
   * {@link Instance} to represent the dimension which will reduced to this
   * hashes.
   * 
   * @param hashSize
   *            the number of used hashes
   */
  public void setHashSize(int hashSize) {
    this.hashSize = hashSize;
  }

  /**
   * Sets the list of attributes indexes which do not should be considered by
   * the {@link Classifier} or {@link Cluster} algorithm. The filter simply
   * sets attributes which should be ignored to all the same value.
   * 
   * @param ignoreAttributes
   *            the list of attribute indexes which should be ignored.
   */
  public void setIgnorableAttributes(ArrayList<Integer> ignoreAttributes) {
    this.ignoreAttributes = ignoreAttributes;
    Collections.sort(this.ignoreAttributes);
  }

  /**
   * Set attributes which should be made "binary". This makes mostly sense for
   * attributes which are also in the
   * {@link ReduceDimensionFilter#notHashableAttributes} list. The Filter
   * simply checks if the value (must be doulbe) is larger than 0 and
   * considers those values as positive --> 1 and all others as 0.
   * 
   * @param makeBinary
   *            list of attributes indexes which should be made binary (0,1)
   */
  public void setMakeBinaryAttributes(ArrayList<Integer> makeBinary) {
    this.makeBinaryAttributes = makeBinary;
    Collections.sort(this.makeBinaryAttributes);
  }

  /**
   * Set the List of IDs of Attributes which should not be used by the
   * HashTrick but stay as single attributes. The id represents the original
   * index of the attribute of the original {@link Instance}
   * 
   * @param notHashableAttributes
   *            List of Attribute Indexes not to reduce using the hashtrick
   */
  public void setNotHashableAttributes(
      ArrayList<Integer> notHashableAttributes) {
    this.notHashableAttributes = notHashableAttributes;
    Collections.sort(this.notHashableAttributes);
  }

  @Override
  public String getPurposeString() {
    return "Reduce the number of attributes, coming from tokens to a fixed size using a hash.";
  }

  /**
   * The new instance header.
   */
  public InstancesHeader getHeader() {
    return new InstancesHeader(newInstances);
  }

  /**
   * Returns the next instances based on the configuration of this class.
   */
  public Instance nextInstance() {
    Instance inst = this.inputStream.nextInstance();

    Instance newInst = new SparseInstance(hashSize
        + notHashableAttributes.size());
    newInst.setDataset(newInstances);
    newInst.replaceMissingValues(replacementArray);
    if (newInstances.size() > 0)
      newInstances.remove(0);
    // newInstances.add(0, newInst);
    for (int i = 0; i < inst.numAttributes(); i++) {
      if (inst.classIndex() == i) {
        newInst.setValue(
            attributesIndex.get(inst.classAttribute().name()),
            inst.classValue());
      } else {
        // check if attributes should be manipulated
        if (ignoreAttributes.contains(i)) {
          inst.setValue(i, 0);
        }
        if (makeBinaryAttributes.contains(i) && inst.value(i) > 0) {
          inst.setValue(i, 1);
        }
        // check what should be done with the attributes.
        if (notHashableAttributes.contains(i)) {
          newInst.setValue(
              attributesIndex.get(inst.attribute(i).name()),
              inst.value(i));

        } else {
          // calculate the hash of the attribute name which is
          // included in
          // the vector and set it to 1
          if (inst.value(i) > 0) {
            newInst.setValue(attributesIndex
                .get(getAttributeNameOfHash(getHash(inst
                    .attribute(i).name(), hashSize))), 1);
          }
        }
      }
    }
    // System.out.println(newInst.toString());
    return newInst;
  }

  /**
   * not implemented.
   */
  public void getDescription(StringBuilder sb, int indent) {
    // we do not need this

  }

  @Override
  protected void restartImpl() {
    init();
  }

  /**
   * Called by {@link ReduceDimensionFilter#restartImpl()}
   */
  private void init() {
    if (this.inputStream == null) {
      System.out.println("Could not find input stream.");
      System.exit(0);
    }
    InstancesHeader oldHeader = this.inputStream.getHeader();
    ArrayList<Attribute> attributes = new ArrayList<Attribute>();
    // the class
    Attribute classAttribute = oldHeader.classAttribute();
    attributes.add(classAttribute);
    // the unchanged attributes
    for (int i = 0; i < notHashableAttributes.size(); i++) {
      attributes
      .add(1, oldHeader.attribute(notHashableAttributes.get(i)));
    }
    // our new elements
    if (hashSize == 0) {
      System.out.println("Max attribute number is 0.");
      System.exit(0);
    }
    for (int i = 0; i < hashSize; i++) {
      attributes.add(notHashableAttributes.size() + 1, new Attribute(
          getAttributeNameOfHash(i), boolAttValues));
    }
    newInstances = new Instances("reduced", attributes, 1);
    newInstances.setClass(classAttribute);
    attributesIndex = new HashMap<String, Integer>();
    for (int i = 0; i < attributes.size(); i++) {
      attributesIndex.put(attributes.get(i).name(), i);
    }
    replacementArray = new double[hashSize + notHashableAttributes.size()];
    for (int i = 0; i < hashSize + notHashableAttributes.size(); i++) {
      replacementArray[i] = 0.0;
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

}
