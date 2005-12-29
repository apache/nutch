/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.logging.Logger;

import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

/**
 * Factory class, which instantiates a Signature implementation according to the
 * current NutchConf configuration. This newly created instance is cached in the
 * NutchConf instance, so that it could be later retrieved.
 * 
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class SignatureFactory {
  private static final Logger LOG =
    LogFormatter.getLogger(SignatureFactory.class.getName());

  private SignatureFactory() {}                   // no public ctor

  /** Return the default Signature implementation. */
  public static Signature getSignature(NutchConf conf) {
    String clazz = conf.get("db.signature.class", MD5Signature.class.getName());
    Signature impl = (Signature)conf.getObject(clazz);
    if (impl == null) {
      try {
        LOG.info("Using Signature impl: " + clazz);
        Class implClass = Class.forName(clazz);
        impl = (Signature)implClass.newInstance();
        impl.setConf(conf);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't create " + clazz, e);
      }
    }
    return impl;
  }
}
