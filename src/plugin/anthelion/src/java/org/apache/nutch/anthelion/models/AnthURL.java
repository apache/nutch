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

import java.net.URI;

/**
 * Representation of an URL in Anthelion.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class AnthURL implements Comparable<AnthURL> {

  public long id;
  public URI uri;
  public boolean semFather;
  public boolean nonSemFather;
  public boolean semSibling;
  public boolean nonSemSibling;
  public boolean sem = false;
  public double prediction;
  public static String UNKNOWNHOST = "unknownhost";

  public AnthURL(URI uri, boolean semFather, boolean nonSemFather,
      boolean semSibling, boolean nonSemSibling) {
    new AnthURL(null, uri, semFather, nonSemFather, semSibling,
        nonSemSibling, false);
  }

  public AnthURL(Long id, URI uri, boolean semFather, boolean nonSemFather,
      boolean semSibling, boolean nonSemSibling) {
    new AnthURL(id, uri, semFather, nonSemFather, semSibling,
        nonSemSibling, false);
  }

  public AnthURL(Long id, URI uri, boolean semFather, boolean nonSemFather,
      boolean semSibling, boolean nonSemSibling, boolean sem) {
    this.id = id;
    this.uri = uri;
    this.semFather = semFather;
    this.semSibling = semSibling;
    // not yet predicted
    this.prediction = 0;
    this.sem = sem;
  }

  public String getHost() {
    return (uri.getHost() != null ? uri.getHost() : UNKNOWNHOST);
  }

  /**
   * This is a little bit weird but as we use a priority queue which returns
   * the smallest element we need to adjust the compareTo in this way.
   */
  @Override
  public int compareTo(AnthURL o) {
    if (o.prediction == prediction) {
      return 0;
    } else if (o.prediction > prediction) {
      return 1;
    } else {
      return -1;
    }

  }

}
