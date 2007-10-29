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

package org.apache.nutch.crawl;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.ObjectCache;

/**
 * Factory class, which instantiates a Signature implementation according to the
 * current Configuration configuration. This newly created instance is cached in the
 * Configuration instance, so that it could be later retrieved.
 * 
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class SignatureFactory {
  private static final Log LOG = LogFactory.getLog(SignatureFactory.class);

  private SignatureFactory() {}                   // no public ctor

  /** Return the default Signature implementation. */
  public static Signature getSignature(Configuration conf) {
    String clazz = conf.get("db.signature.class", MD5Signature.class.getName());
    ObjectCache objectCache = ObjectCache.get(conf);
    Signature impl = (Signature)objectCache.getObject(clazz);
    if (impl == null) {
      try {
        if (LOG.isInfoEnabled()) {
          LOG.info("Using Signature impl: " + clazz);
        }
        Class implClass = Class.forName(clazz);
        impl = (Signature)implClass.newInstance();
        impl.setConf(conf);
        objectCache.setObject(clazz, impl);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't create " + clazz, e);
      }
    }
    return impl;
  }
}
