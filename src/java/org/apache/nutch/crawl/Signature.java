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

import org.apache.nutch.parse.Parse;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConf;
import org.apache.nutch.util.NutchConfigurable;

public abstract class Signature implements NutchConfigurable {
  protected NutchConf conf;
  
  public abstract byte[] calculate(Content content, Parse parse);

  public NutchConf getConf() {
    return conf;
  }

  public void setConf(NutchConf conf) {
    this.conf = conf;
  }
}
