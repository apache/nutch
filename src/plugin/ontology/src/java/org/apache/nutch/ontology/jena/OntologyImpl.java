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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.ontology.*;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;

import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.ontology.OntResource;
import com.hp.hpl.jena.ontology.Restriction;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.shared.PrefixMapping;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;


import java.io.PrintStream;

/**
 * this class wraps about a model, 
 * built from a list of ontologies,
 * uses HP's Jena
 *
 * @author michael j pan
 */
public class OntologyImpl implements org.apache.nutch.ontology.Ontology {
  public static final Log LOG = LogFactory.getLog("org.apache.nutch.ontology.Ontology");

  public final static String DELIMITER_SEARCHTERM = " ";

  private static Hashtable searchTerms = new Hashtable();
  private static Parser parser;

  //private static Object ontologyModel;
  private static OntModel ontologyModel;

  private static Ontology ontology = null;

  private static Map m_anonIDs = new HashMap();
  private static int m_anonCount = 0;

  public OntologyImpl() {
    //only initialize all the static variables
    //if first time called to this ontology constructor
    if (ontology == null) {
      if (LOG.isInfoEnabled()) { LOG.info( "creating new ontology"); }
      parser = new OwlParser();
      ontology = this;
    }

    if (ontologyModel == null)
      ontologyModel =
        ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, null);
        //ModelFactory.createOntologyModel();
  }

  public static Ontology getInstance () {
    if (ontology == null) {
      //ontology = new org.apache.nutch.ontology.Ontology();
      ontology = new org.apache.nutch.ontology.jena.OntologyImpl();
    }
    return ontology;
  }

  public void load (String[] urls) {
    for (int i=0; i<urls.length; i++) {
      String url = urls[i].trim();
      if (!url.equals(""))
        load(ontologyModel, url);
    }
    parser.parse(ontologyModel);
  }

  private void load (Object m, String url) {
    try {
      if (LOG.isInfoEnabled()) { LOG.info( "reading "+url); }
      ((OntModel)m).read(url);
    } catch (Exception e) {
      if (LOG.isFatalEnabled()) {
        LOG.fatal("failed on attempting to read ontology "+url);
        LOG.fatal(e.getMessage());
        e.printStackTrace(LogUtil.getFatalStream(LOG));
      }
    }
  }

  public static Parser getParser() {
    if (parser == null) {
      parser = new OwlParser();
    }
    return parser;
  }

  public static OntModel getModel() {
    return (OntModel)ontologyModel;
  }

  // not yet implemented
  //public void merge (org.apache.nutch.ontology.Ontology o) {
  //}

  /**
   * retrieve all subclasses of entity(ies) hashed to searchTerm
   */
  public Iterator subclasses (String entitySearchTerm) {
    Map classMap = retrieve(entitySearchTerm);
    Map subclasses = new HashMap();
  
    Iterator iter = classMap.keySet().iterator();
    while (iter.hasNext()) {
      //OntClass resource = (OntClass) iter.next();
      OntResource resource = (OntResource) iter.next();
  
      if (resource instanceof OntClass) {
        //get subclasses
        for (Iterator i=((OntClass)resource).listSubClasses(); i.hasNext();) {
          OntResource subclass = (OntResource) i.next();
          for (Iterator j=subclass.listLabels(null); j.hasNext();) {
            Literal l = (Literal) j.next();
            subclasses.put(l.toString(), "1");
          }
        }
        //get individuals
        for (Iterator i=((OntClass)resource).listInstances(); i.hasNext();) {
          OntResource subclass = (OntResource) i.next();
          for (Iterator j=subclass.listLabels(null); j.hasNext();) {
            Literal l = (Literal) j.next();
            subclasses.put(l.toString(), "1");
          }
        }
      } else if (resource instanceof Individual) {
        for (Iterator i=resource.listSameAs(); i.hasNext();) {  
          OntResource subclass = (OntResource) i.next();
          for (Iterator j=subclass.listLabels(null); j.hasNext();) {
            Literal l = (Literal) j.next();
            subclasses.put(l.toString(), "1");
          }    
        }
      }
    }

    return subclasses.keySet().iterator();
  }

  /**
   * retrieves synonyms from wordnet via sweet's web interface
   */
  public Iterator synonyms (String queryKeyPhrase) {
    //need to have a html quote method instead
    queryKeyPhrase = queryKeyPhrase.replaceAll("\\s+", "\\+");

    Map classMap = retrieve(queryKeyPhrase);

    Map synonyms = new HashMap();

    Iterator iter = classMap.keySet().iterator();
    while (iter.hasNext()) {
      OntResource resource = (OntResource) iter.next();

      //listLabels
      for (Iterator i=resource.listLabels(null); i.hasNext();) {
        Literal l = (Literal) i.next();
        synonyms.put(l.toString(), "1");
      }
    
      if (resource instanceof Individual) {
      //get all individuals same as this one
        for (Iterator i=resource.listSameAs(); i.hasNext();) {
          Individual individual = (Individual) i.next();
          //add labels
          for (Iterator j =individual.listLabels(null); j.hasNext();) {
            Literal l = (Literal) i.next();
            synonyms.put(l.toString(), "1");
          }
        }
      } else if (resource instanceof OntClass) {
        //list equivalent classes
        for (Iterator i=((OntClass)resource).listEquivalentClasses();
          i.hasNext();) {
          OntClass equivClass = (OntClass) i.next();
          //add labels
          for (Iterator j=equivClass.listLabels(null); j.hasNext();) {
            Literal l = (Literal) j.next();
            synonyms.put(l.toString(), "1");
          }
        }
      }
    }

    return synonyms.keySet().iterator();
  }

  public static void addSearchTerm(String label, OntResource resource) {
    Map m = retrieve(label);
    if (m == null) {
      m=new HashMap();
    }
    m.put(resource, "1");
    searchTerms.put(label.toLowerCase(), m);
  }

  public static Map retrieve(String label) {
    Map m = (Map) searchTerms.get(label.toLowerCase());
    if (m==null) {
      m = new HashMap();
    }
    return m;
  }

  protected static void renderHierarchy( PrintStream out, OntClass cls, 
            List occurs, int depth ) {
    renderClassDescription( out, cls, depth );
    out.println();
  
    // recurse to the next level down
    if (cls.canAs( OntClass.class ) && !occurs.contains( cls )) {
      for (Iterator i = cls.listSubClasses( true ); i.hasNext(); ) {
        OntClass sub = (OntClass) i.next();

        // we push this expression on the occurs list before we recurse
        occurs.add( cls );
        renderHierarchy( out, sub, occurs, depth + 1 );
        occurs.remove( cls );
      }
      for (Iterator i=cls.listInstances(); i.hasNext(); ) {
        Individual individual = (Individual) i.next();
        renderURI(out, individual.getModel(), individual.getURI());
        out.print(" [");
        for (Iterator j=individual.listLabels(null); j.hasNext();) {
          out.print(((Literal)j.next()).getString()+", ");
        }
        out.print("] ");
        out.println();
      }
    }
  }

  public static void renderClassDescription( PrintStream out, 
    OntClass c, int depth ) {
    indent( out, depth );
    
    if (c.isRestriction()) {
      renderRestriction( out, (Restriction) c.as( Restriction.class ) );
    } else {
      if (!c.isAnon()) {
        out.print( "Class " );
        //renderURI( out, c.getModel(), c.getURI() );

        out.print (c.getLocalName());

        out.print( " [" );
        for (Iterator i=c.listLabels(null); i.hasNext(); ) {
          out.print(((Literal)i.next()).getString()+", ");
        }
        out.print( "] ");
      } else {
        renderAnonymous( out, c, "class" );
      }
    }
  }
  
  protected static void renderRestriction( PrintStream out, Restriction r ) {
    if (!r.isAnon()) {
      out.print( "Restriction " );
      renderURI( out, r.getModel(), r.getURI() );
    } else {
      renderAnonymous( out, r, "restriction" );
    }
    
    out.print( " on property " );
    renderURI( out, r.getModel(), r.getOnProperty().getURI() );
  }

  protected static void renderURI( PrintStream out, 
    PrefixMapping prefixes, String uri ) {
    out.print( prefixes.usePrefix( uri ) );
  }
  
  protected static void renderAnonymous( PrintStream out, 
    Resource anon, String name ) {
    String anonID = (String) m_anonIDs.get( anon.getId() );
    if (anonID == null) {
      anonID = "a-" + m_anonCount++;
      m_anonIDs.put( anon.getId(), anonID );
    }
    
    out.print( "Anonymous ");
    out.print( name );
    out.print( " with ID " );
    out.print( anonID );
  }
  
  protected static void indent( PrintStream out, int depth ) {
    for (int i = 0; i < depth; i++) {
      out.print( " " );
    }
  }

  public static void main( String[] args ) throws Exception {

    Configuration conf = NutchConfiguration.create(); 
    Ontology ontology = new OntologyFactory(conf).getOntology();

    String urls = conf.get("extension.ontology.urls");
    if (urls==null || urls.trim().equals("")) {
      if (LOG.isFatalEnabled()) { LOG.fatal("No ontology url found."); }
      return;
    }
    ontology.load(urls.split("\\s+"));
    if (LOG.isInfoEnabled()) { LOG.info( "created new ontology"); }
    
    for (Iterator i = getParser().rootClasses( getModel() ); 
      i.hasNext(); ) {
    
      //print class
      OntClass c = (OntClass) i.next();

      renderHierarchy(System.out, c, new LinkedList(), 0);
    }

    String[] terms =
      new String[] { "Season" };

    for (int i=0; i<terms.length; i++) {
      Iterator iter = ontology.subclasses(terms[i]);
      while (iter.hasNext()) {
        System.out.println("subclass >> "+(String)iter.next());
      }
    }
  }
}
