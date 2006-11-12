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
package org.apache.nutch.spell;

public class SpellCheckerTerm {
  String originalTerm = null;

  String charsBefore = null;

  String charsAfter = null;

  String suggestedTerm = null;

  int originalDocFreq = -1;

  int suggestedTermDocFreq = -1;

  boolean isMispelled = false;

  public SpellCheckerTerm(String originalTerm, String charsBefore,
      String charsAfter) {
    super();
    this.originalTerm = originalTerm;
    this.charsBefore = charsBefore;
    this.charsAfter = charsAfter;
  }

  public String getCharsAfter() {
    return charsAfter;
  }

  public void setCharsAfter(String charsAfter) {
    this.charsAfter = charsAfter;
  }

  public String getCharsBefore() {
    return charsBefore;
  }

  public void setCharsBefore(String charsBefore) {
    this.charsBefore = charsBefore;
  }

  public int getOriginalDocFreq() {
    return originalDocFreq;
  }

  public void setOriginalDocFreq(int originalDocFreq) {
    this.originalDocFreq = originalDocFreq;
  }

  public String getOriginalTerm() {
    return originalTerm;
  }

  public void setOriginalTerm(String originalTerm) {
    this.originalTerm = originalTerm;
  }

  public int getSuggestedTermDocFreq() {
    return suggestedTermDocFreq;
  }

  public void setSuggestedTermDocFreq(int suggestedTermDocFreq) {
    this.suggestedTermDocFreq = suggestedTermDocFreq;
  }

  public String getSuggestedTerm() {
    return suggestedTerm;
  }

  public void setSuggestedTerm(String suggestedTerm) {
    this.suggestedTerm = suggestedTerm;
  }

  public boolean isMispelled() {
    return isMispelled;
  }

  public void setMispelled(boolean isMispelled) {
    this.isMispelled = isMispelled;
  }

  public String toString() {
    return "[" + originalTerm + ", " + charsBefore + ", " + charsAfter + ", "
        + suggestedTerm + ", " + originalDocFreq + ", " + suggestedTermDocFreq
        + ", " + isMispelled + "]";
  }
}
