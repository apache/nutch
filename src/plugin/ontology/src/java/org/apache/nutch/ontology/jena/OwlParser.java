/**
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

package org.apache.nutch.ontology.jena;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.rdf.model.Literal;
import org.apache.nutch.ontology.*;
import org.apache.nutch.util.LogUtil;

/**
 * implementation of parser for w3c's OWL files
 *
 * @author michael j pan
 */
public class OwlParser implements Parser {

  private final static Log LOG = LogFactory.getLog(OwlParser.class);

  public OwlParser () {
  }

  /**
   * parse owl ontology files using jena
   */
  public void parse(OntModel m) {
    for (Iterator i = rootClasses( m );  i.hasNext();  ) {
      OntClass c = (OntClass) i.next();

      //dont deal with anonymous classes
      if (c.isAnon()) {
        continue;
      }

      parseClass( c,  new ArrayList(), 0 );
    }
  }

  protected  void parseClass( OntClass cls, List occurs, int depth ) {
    //dont deal with anonymous classes
    if (cls.isAnon()) {
      return;
    }

    //add cls to Ontology searchterms
    //list labels
    Iterator labelIter = cls.listLabels(null);
    //if has no labels
    if (!labelIter.hasNext()) {
        //add rdf:ID as a label
        cls.addLabel(rdfidToLabel(cls.getLocalName()), null);
    }    
    //reset the label iterator
    labelIter = cls.listLabels(null);
  
    while(labelIter.hasNext()) {
      Literal l = (Literal) labelIter.next();
      OntologyImpl.addSearchTerm(l.toString(), cls);
    }

    // recurse to the next level down
    if (cls.canAs( OntClass.class )  &&  !occurs.contains( cls )) {
      //list subclasses
      for (Iterator i = cls.listSubClasses( true );  i.hasNext(); ) {
        OntClass sub = (OntClass) i.next();

        // we push this expression on the occurs list before we recurse
        occurs.add( cls );
        parseClass(sub, occurs, depth+1);
        occurs.remove( cls );
      }

      //list instances
      for (Iterator i=cls.listInstances(); i.hasNext(); ) {
        //add search terms for each instance

        //list labels
        Individual individual = (Individual) i.next();
        for (Iterator j=individual.listLabels(null); j.hasNext();) {
          Literal l = (Literal) j.next();
          OntologyImpl.addSearchTerm(l.toString(), individual);
        }
      }
    }
  }

  public Iterator rootClasses( OntModel m ) {
    List roots = new ArrayList();
    
    for (Iterator i = m.listClasses();  i.hasNext(); ) {
      OntClass c = (OntClass) i.next();
        
      try {
      // too confusing to list all the restrictions as root classes 
        if (c.isAnon()) {
          continue;
        }
    
        if (c.hasSuperClass( m.getProfile().THING(), true ) ) {
          // this class is directly descended from Thing
          roots.add( c );
        } else if (c.getCardinality( m.getProfile().SUB_CLASS_OF() ) == 0 ) {
          // this class has no super-classes
          // (can occur if we're not using the reasoner)
          roots.add( c );
        }
      } catch (Exception e) {
        e.printStackTrace(LogUtil.getWarnStream(LOG));
      }
    }
    
    return roots.iterator();
  }

  public String rdfidToLabel (String idString) {
    Pattern p = Pattern.compile("([a-z0-9])([A-Z])");
    Matcher m = p.matcher(idString);

    String labelString = new String(idString);
    while(m.find()) {
      labelString = labelString.replaceAll(m.group(1)+m.group(2),
             m.group(1)+" "+m.group(2));
    }
    return labelString;
  }

}
