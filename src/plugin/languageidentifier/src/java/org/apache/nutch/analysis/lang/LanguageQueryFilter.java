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

package org.apache.nutch.analysis.lang;

import org.apache.nutch.searcher.RawFieldQueryFilter;

/**
 * A {@link org.apache.nutch.searcher.QueryFilter} that handles
 * <code>"lang:"</code> query clauses.
 * It search the <code>"lang"</code> field indexed by the
 * LanguageIdentifier.
 *
 * @author Sami Siren
 * @author Jerome Charron
 */
public class LanguageQueryFilter extends RawFieldQueryFilter {

  public LanguageQueryFilter() {
    super("lang");
  }

}
