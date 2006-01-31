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

package org.apache.nutch.net;

import org.apache.nutch.util.*;

import java.net.URL;
import java.util.logging.*;

/** Factory to create a UrlNormalizer from "urlnormalizer.class" config property. */
public class UrlNormalizerFactory {
  private static final Logger LOG = LogFormatter
      .getLogger("org.apache.nutch.net.UrlNormalizerFactory");

  private NutchConf nutchConf;

  public UrlNormalizerFactory(NutchConf nutchConf) {
    this.nutchConf = nutchConf;
  }

  /** Return the default UrlNormalizer implementation. */
  public UrlNormalizer getNormalizer() {
    String urlNormalizer = null;
    UrlNormalizer normalizer = (UrlNormalizer) this.nutchConf
        .getObject(UrlNormalizer.class.getName());
    if (normalizer == null) {
      try {
        urlNormalizer = this.nutchConf.get("urlnormalizer.class");
        LOG.info("Using URL normalizer: " + urlNormalizer);
        Class normalizerClass = Class.forName(urlNormalizer);
        normalizer = (UrlNormalizer) normalizerClass.newInstance();
        normalizer.setConf(this.nutchConf);
        this.nutchConf.setObject(UrlNormalizer.class.getName(), normalizer);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't create " + urlNormalizer, e);
      }
    }
    return normalizer;
  }
}
