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
package org.apache.nutch.indexer.arbitrary;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;

import org.apache.hadoop.io.Text;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.Class;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;

/**
 * Adds arbitrary searchable fields to a document from the class and method
 * the user identifies in the config. The user supplies the name of the field
 * to add with the class and method names that supply the value.
 * 
 * Example:<br><br>
 * &lt;property&gt;<br>
 *   &lt;name&gt;index.arbitrary.function.count&lt;/name&gt;<br>
 *   &lt;value&gt;1&lt;/value&gt;<br>
 * &lt;/property&gt;<br>
 * <br>
 * &lt;property&gt;<br>
 *   &lt;name&gt;index.arbitrary.fieldName.0&lt;/name&gt;<br>
 *   &lt;value&gt;advisors&lt;/value&gt;<br>
 * &lt;/property&gt;<br>
 * <br>
 * &lt;property&gt;<br>
 *   &lt;name&gt;index.arbitrary.className.0&lt;/name&gt;<br>
 *   &lt;value&gt;com.example.arbitrary.AdvisorCalculator&lt;/value&gt;<br>
 * &lt;/property&gt;<br>
 * <br>
 * &lt;property&gt;<br>
 *   &lt;name&gt;index.arbitrary.constructorArgs.0&lt;/name&gt;<br>
 *   &lt;value&gt;Kirk&lt;/value&gt;<br>
 * &lt;/property&gt;<br>
 * <br>
 * &lt;property&gt;<br>
 *   &lt;name&gt;index.arbitrary.methodName.0&lt;/name&gt;<br>
 *   &lt;value&gt;countAdvisors&lt;/value&gt;<br>
 * &lt;/property&gt;<br>
 * <br>
 * &lt;property&gt;<br>
 *   &lt;name&gt;index.arbitrary.methodArgs.0&lt;/name&gt;<br>
 *   &lt;value&gt;Spock,McCoy&lt;/value&gt;<br>
 * &lt;/property&gt;<br>
 * <br>
 * To set more than one arbitrary field value,
 * increment {@code index.arbitrary.function.count} and
 * repeat the rest of these blocks with successive int values
 * appended to the property names, e.g. fieldName.1, methodName.1, etc.
 */
public class ArbitraryIndexingFilter implements IndexingFilter {

  private static final Logger LOG = LoggerFactory
    .getLogger(MethodHandles.lookup().lookupClass());

  /** How many arbitrary field definitions to set. */
  private int arbitraryAddsCount = 0;
  
  /** The name of the field to insert/overwrite in the NutchDocument */
  private String fieldName;
  
  /** The fully-qualified class name of the custom class to use for the
   *  new field. This class must be in the Nutch runtime classpath,
   *  e.g., nutch/lib/ dierctory. */
  private String className;
  
  /** The String values to pass to the custom class constructor. The plugin
   *  will add the document url as the first argument in className's
   *  String[] args. */
  private String[] userConstrArgs;
  
  /** The array where the plugin copies the url &amp; the userConstrArgs
   *  to create the instance of className. */
  private String[] constrArgs;
  
  /** The name of the method in the custom class to call. Its return value
   *  will become the value of fieldName in the NutchDocument. */
  private String methodName;
  
  /** The String values of the arguments to methodName. It's up to the
   *  developer of className to do any casts/conversions from String to
   *  another class in the code of className. */
  private String[] methodArgs;
  
  /** The result that returns from methodName. The plugin will set the value
   *  of fieldName to this. */
  private Object result;
  
  /** Optional flag to determine whether to overwrite the existing value in the
   *  NutchDocument fieldName if this is set to true. Default behavior is to
   *  add the value from calling methodName to existing values for fieldName. */
  private boolean overwrite = false;
  
  /** Hadoop Configuration object to pass these values into the plugin. */
  private Configuration conf;

  /**
   * The {@link ArbitraryIndexingFilter} filter object uses reflection
   * to instantiate the configured class and invoke the configured method.
   * It requires a few configuration settings for adding arbitrary fields
   * and values to the NutchDocument as searchable fields.
   * See {@code index.arbitrary.function.count}, and (possibly multiple
   * instances when {@code index.arbitrary.function.count} &gt; 1) of the following
   * {@code index.arbitrary.fieldName}.<em>index</em>,
   * {@code index.arbitrary.className}.<em>index</em>,
   * {@code index.arbitrary.constructorArgs}.<em>index</em>,
   * {@code index.arbitrary.methodName}.<em>index</em>, and
   * {@code index.arbitrary.methodArgs}.<em>index</em>
   * in nutch-default.xml or nutch-site.xml where <em>index</em> ranges from 0
   * to {@code index.arbitrary.function.count} - 1.
   * 
   * @param doc
   *          The {@link NutchDocument} object
   * @param parse
   *          The relevant {@link Parse} object passing through the filter
   * @param url
   *          URL to be filtered by the user-specified class
   * @param datum
   *          The {@link CrawlDatum} entry
   * @param inlinks
   *          The {@link Inlinks} containing anchor text
   * @return filtered NutchDocument
   */
  @Override
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
                              CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    Class theClass = null;
    Method theMethod = null;
    Constructor<?> theConstructor = null;
    Object instance = null;

    // This'll be quick
    if (doc == null) {
      LOG.debug("In filter() where doc is null for url == {}",
               String.valueOf(url));
      return doc;
    } else if (url == null) {
      LOG.debug("In filter() where url is null. Nothing to do.");
      return doc;
    }

    int cfgCounter = 0;
    while (cfgCounter <  arbitraryAddsCount) {
      setIndexedConf(conf,cfgCounter);
      cfgCounter++;
      try {
        theClass = Class.forName(className);
        if (methodArgs.length > 0) {
          theMethod = theClass.getDeclaredMethod(methodName,String[].class);
        } else {
          theMethod = theClass.getMethod(methodName);
        }
        theConstructor = theClass.getDeclaredConstructor(String[].class);
      } catch (Exception e) {
        LOG.error("Exception preparing reflection tasks. className was {}",
		 String.valueOf(className));
        e.printStackTrace();
      }
      try {
        constrArgs = new String[userConstrArgs.length + 1];
        constrArgs[0] = url.toString();
        System.arraycopy(userConstrArgs,0,constrArgs,1,userConstrArgs.length);
        instance = theConstructor.newInstance(new Object[]{constrArgs});
        if (methodArgs.length > 0) {
          result = theMethod.invoke(instance, new Object[]{methodArgs});
        } else {
          result = theMethod.invoke(instance);
        }
      } catch (Exception e) {
        LOG.error("Exception in reflection trying to instantiate/invoke. "
		  + "url was {} & className was {}",
		  String.valueOf(url), String.valueOf(className));
        if (constrArgs.length > 0) {
          LOG.error("constrArgs[1] was {}", String.valueOf(constrArgs[1]));
        }
        LOG.error("methodName was {}", String.valueOf(className));
        if (methodArgs.length > 0) {
          LOG.error("methodArgs[0] was {}", String.valueOf(methodArgs[0]));
        }
        e.printStackTrace();
      }

      LOG.debug("{}.{}() returned {} for field {}.", className,
		methodName, String.valueOf(result), String.valueOf(fieldName));
      
      // If user chose to overwrite, remove existing value
      if (overwrite) {
	LOG.debug("overwrite == true for fieldName == {} ", fieldName);
	if (doc.getFieldNames().contains(fieldName)) {
	  LOG.debug("Removing field '{}' from doc for overwrite", fieldName);
	  doc.removeField(fieldName);
	}
      }
      if (result == null) {
        LOG.debug("Call to {}.{} returned null", className, methodName);
        if (overwrite) {
          LOG.debug("{} has been cleared.", fieldName);
        }
      }
      LOG.debug("Adding value '{}' for field '{}' to doc", result, fieldName);
      doc.add(fieldName, result);
    }
    return doc;
  }

  /**
   * Set the {@link Configuration} object
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    arbitraryAddsCount = conf.getInt("index.arbitrary.function.count",1);
    LOG.info("Will process the first {} fieldName defs in config.", String.valueOf(arbitraryAddsCount));
  }

  /**
   * Set the {@link Configuration} object for a specific set of values in the config
   *
   * @param conf
   *        The Configuration object holding values for the current arbitrary field.
   * @param ndx
   *          The ordinal counter value for the current arbitrary field appended to the
   *          base property names in the xml configuration file.
   */
  public void setIndexedConf(Configuration conf, int ndx) {
    LOG.debug("In setIndexedConf() where ndx was passed in as {}", String.valueOf(ndx));
    fieldName = conf.get("index.arbitrary.fieldName.".concat(String.valueOf(ndx)));
    LOG.debug("Looking now for index.arbitrary.fieldname.{} which was: {}",
	      String.valueOf(ndx),String.valueOf(fieldName));

    if (fieldName == null || fieldName == "") {
      throw new RuntimeException ("Problem in configuration where the index.arbitrary.fieldName."
                                  + String.valueOf(ndx) + " is missing.");
    }

    className = conf.get("index.arbitrary.className.".concat(String.valueOf(ndx)));
    if (className == null || className == "") {
      throw new RuntimeException ("Problem in configuration where the index.arbitrary.className."
                                  + String.valueOf(ndx) + " is missing.");
    }

    userConstrArgs = conf.getTrimmedStrings("index.arbitrary.constructorArgs.".concat(String.valueOf(ndx)));
    methodName = conf.get("index.arbitrary.methodName.".concat(String.valueOf(ndx)),"");
    methodArgs = conf.getTrimmedStrings("index.arbitrary.methodArgs.".concat(String.valueOf(ndx)));
    overwrite = conf.getBoolean("index.arbitrary.overwrite.".concat(String.valueOf(ndx)),false);
    if (overwrite) {
      LOG.info("overwrite set == true for processing {}.", fieldName);
    }
  }

  /**
   * Get the {@link Configuration} object */
  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
