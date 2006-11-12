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

import java.util.ArrayList;

public class SpellCheckerTerms  {
  boolean hasMispelledTerms = false;
  ArrayList terms=new ArrayList();

  public SpellCheckerTerm getSpellCheckerTerm(int i) {
    return (SpellCheckerTerm) terms.get(i);
  }

  public ArrayList getTerms(){
    return terms;
  }
  
  public String getSpellCheckedQuery() {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < terms.size(); i++) {
      SpellCheckerTerm term = getSpellCheckerTerm(i);
      buf.append(term.charsBefore);
      if (term.isMispelled)
        buf.append(term.suggestedTerm);
      else
        buf.append(term.originalTerm);
      buf.append(term.charsAfter);
    }
    return buf.toString();
  }

  public boolean getHasMispelledTerms() {
    return hasMispelledTerms;
  }

  public void setHasMispelledTerms(boolean hasMispelledTerms) {
    this.hasMispelledTerms = hasMispelledTerms;
  }

  public Object get(int index) {
    return terms.get(index);
  }

  public int size() {
    return terms.size();
  }

  public boolean add(Object arg0) {
    return terms.add(arg0);
  }
}
