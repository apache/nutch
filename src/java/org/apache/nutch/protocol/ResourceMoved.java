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

package org.apache.nutch.protocol;

import java.io.IOException;
import java.net.URL;

/** Thrown by {@link Protocol#getContent(String)} when a {@link URL} no longer
 * exists.*/
public class ResourceMoved extends IOException {
  private URL oldUrl;
  private URL newUrl;

  public ResourceMoved(URL oldUrl, URL newUrl, String message) {
    super(message);
    this.newUrl = newUrl;
    this.oldUrl = oldUrl;
  }

  public URL getNewUrl() { return newUrl; }
  public URL getOldUrl() { return oldUrl; }
}
